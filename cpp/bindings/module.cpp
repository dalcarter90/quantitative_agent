#include "qa/clock.hpp"
#include "qa/events.hpp"
#include "qa/notional.hpp"
#include "qa/order_book.hpp"
#include "qa/types.hpp"
#include "qa/version.hpp"

#include <pybind11/pybind11.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <optional>
#include <type_traits>
#include <vector>

namespace py = pybind11;

namespace {

// --- integers in from Python: exact, in range, never float or bool ----------------

template <class T>
T int_from(py::handle value, const char* what) {
  static_assert(std::is_integral_v<T>);
  PyObject* object = value.ptr();
  if (!PyLong_Check(object) || PyBool_Check(object)) {
    throw py::type_error(std::string(what) + " must be an int, not " +
                         Py_TYPE(object)->tp_name);
  }
  if constexpr (std::is_signed_v<T>) {
    int overflow = 0;
    const long long result = PyLong_AsLongLongAndOverflow(object, &overflow);
    if (result == -1 && PyErr_Occurred()) {
      throw py::error_already_set();
    }
    if (overflow != 0 || result < std::numeric_limits<T>::min() ||
        result > std::numeric_limits<T>::max()) {
      throw std::overflow_error(std::string(what) + " is out of range");
    }
    return static_cast<T>(result);
  } else {
    if (PyObject_RichCompareBool(object, py::int_(0).ptr(), Py_LT) == 1) {
      throw std::overflow_error(std::string(what) + " must not be negative");
    }
    const unsigned long long result = PyLong_AsUnsignedLongLong(object);
    if (result == static_cast<unsigned long long>(-1) && PyErr_Occurred()) {
      PyErr_Clear();
      throw std::overflow_error(std::string(what) + " is out of range");
    }
    if (result > std::numeric_limits<T>::max()) {
      throw std::overflow_error(std::string(what) + " is out of range");
    }
    return static_cast<T>(result);
  }
}

py::object not_implemented() { return py::reinterpret_borrow<py::object>(Py_NotImplemented); }

// A strong int64 type in Python: constructed from an int, read with .value,
// ordered only against its own type. No __int__, __index__ or __float__.
template <class T, std::int64_t (T::*Get)() const noexcept>
void bind_strong(py::module_& m, const char* name, const char* doc) {
  py::class_<T>(m, name, doc)
      .def(py::init([name](py::handle value) { return T(int_from<std::int64_t>(value, name)); }),
           py::arg("value"))
      .def_property_readonly("value", [](const T& self) { return (self.*Get)(); })
      .def("__eq__",
           [](const T& self, py::handle other) -> py::object {
             if (!py::isinstance<T>(other)) {
               return not_implemented();
             }
             return py::bool_(self == other.cast<const T&>());
           })
      .def("__ne__",
           [](const T& self, py::handle other) -> py::object {
             if (!py::isinstance<T>(other)) {
               return not_implemented();
             }
             return py::bool_(self != other.cast<const T&>());
           })
      .def("__lt__", [](const T& a, const T& b) { return a < b; })
      .def("__le__", [](const T& a, const T& b) { return a <= b; })
      .def("__gt__", [](const T& a, const T& b) { return a > b; })
      .def("__ge__", [](const T& a, const T& b) { return a >= b; })
      .def("__hash__", [](const T& self) { return py::hash(py::int_((self.*Get)())); })
      .def("__repr__", [name](const T& self) {
        return std::string(name) + "(" + std::to_string((self.*Get)()) + ")";
      });
}

// --- messages: bytes, layout, and validation of what comes back in ----------------------

template <class T>
py::bytes to_bytes(const T& value) {
  return py::bytes(reinterpret_cast<const char*>(&value), sizeof(T));
}

template <class T>
T from_bytes(const py::bytes& raw) {
  const std::string data = raw;
  if (data.size() != sizeof(T)) {
    throw py::value_error("expected " + std::to_string(sizeof(T)) + " bytes, got " +
                          std::to_string(data.size()));
  }
  T value;
  std::memcpy(&value, data.data(), sizeof(T));
  return value;
}

template <std::size_t N>
void require_zero(const std::uint8_t (&reserved)[N]) {
  for (const std::uint8_t byte : reserved) {
    if (byte != 0) {
      throw py::value_error("reserved bytes must be zero");
    }
  }
}

void require_side(qa::Side side) {
  const auto value = static_cast<int>(side);
  if (value < -1 || value > 1) {
    throw py::value_error("side " + std::to_string(value) + " is not a Side");
  }
}

template <class E>
void require_at_most(E value, E last, const char* what) {
  if (static_cast<int>(value) > static_cast<int>(last)) {
    throw py::value_error(std::string(what) + " " + std::to_string(static_cast<int>(value)) +
                          " is not a known value");
  }
}

// Review D7 (ADR 0020): an exponent is 0 to 18, as in the store's schema.
std::int8_t exponent_from(py::handle value, const char* what) {
  const auto exponent = int_from<std::int8_t>(value, what);
  if (exponent < 0 || exponent > 18) {
    throw py::value_error(std::string(what) + " " + std::to_string(exponent) +
                          " must be 0 to 18");
  }
  return exponent;
}

// ADR 0019: a precision is 0 to 18 and never above its exponent, as in the
// store's schema.
std::int8_t precision_from(py::handle value, std::int8_t exponent, const char* what) {
  const auto precision = int_from<std::int8_t>(value, what);
  if (precision < 0 || precision > 18 || precision > exponent) {
    throw py::value_error(std::string(what) + " " + std::to_string(precision) +
                          " must be 0 to 18 and at most its exponent " + std::to_string(exponent));
  }
  return precision;
}

// ADR 0019, ADR 0022: the scales an InstrumentUpdate carries, as the store's
// schema and the InstrumentMeta binding allow them.
void require_scales(const qa::InstrumentMeta& meta, const char* whose) {
  const auto exponent = [whose](std::int8_t value, const char* name) {
    if (value < 0 || value > qa::kMaxExponent) {
      throw py::value_error(std::string(whose) + " " + name + " " + std::to_string(value) +
                            " is not an exponent: an exponent is 0 to 18");
    }
  };
  const auto precision = [whose](std::int8_t value, std::int8_t of, const char* name) {
    if (value < 0 || value > of) {
      throw py::value_error(std::string(whose) + " " + name + " " + std::to_string(value) +
                            " is not a precision of exponent " + std::to_string(of) +
                            ": a precision is 0 to its exponent");
    }
  };
  exponent(meta.price_exp, "price_exp");
  exponent(meta.qty_exp, "qty_exp");
  precision(meta.price_precision, meta.price_exp, "price_precision");
  precision(meta.qty_precision, meta.qty_exp, "qty_precision");
}

// ADR 0020: what MarketEvent.from_bytes refuses beyond unknown enum values.
void require_well_formed(const qa::MarketEvent& event) {
  if ((event.flags & ~qa::MarketEvent::kKnownFlags) != 0) {  // review D7
    throw py::value_error("flags " + std::to_string(event.flags) +
                          " set a bit no flag is assigned to");
  }
  if (!qa::reason_fits(event.kind, event.reason)) {
    throw py::value_error("reason " + std::to_string(static_cast<int>(event.reason)) +
                          " does not fit kind " + std::to_string(static_cast<int>(event.kind)) +
                          ": a stale or reset event needs its own kind of reason, and nothing "
                          "else has one");
  }
  if (qa::is_stream_control(event.kind) &&
      (event.price != qa::Price{} || event.qty != qa::Qty{} || event.side != qa::Side::Unknown ||
       event.trade_id != 0 || event.checksum != 0 || event.depth != 0)) {
    throw py::value_error(
        "a control event carries no price, quantity, side, trade id, checksum or depth");
  }
  if (event.kind == qa::EventKind::InstrumentUpdate) {  // ADR 0022
    const qa::InstrumentMeta meta = qa::instrument_meta(event);
    require_scales(meta, "an instrument update's");
    if (meta.tick_size.mantissa() < 1 || meta.lot_size.mantissa() < 1) {  // ADR 0023
      throw py::value_error("an instrument update's tick size " +
                            std::to_string(meta.tick_size.mantissa()) + " and lot size " +
                            std::to_string(meta.lot_size.mantissa()) +
                            " must each be at least 1, as the store's schema requires");
    }
    if (event.side != qa::Side::Unknown || event.venue_seq != 0 || event.trade_id != 0 ||
        event.checksum != 0 || event.depth != 0 ||
        (event.flags & ~qa::MarketEvent::kInstrumentUpdateFlags) != 0) {
      throw py::value_error(
          "an instrument update carries no side, venue number, trade id, checksum or depth, "
          "and no flag but HAS_MONO and EVENT_TS_FROM_RECEIPT");
    }
  }
}

template <class E>
std::string enum_name(E value) {
  return py::cast(value).attr("name").template cast<std::string>();
}

py::object level_or_none(const std::optional<qa::BookLevel>& level) {
  if (!level) {
    return py::none();
  }
  return py::make_tuple(level->price.mantissa(), level->qty.mantissa());
}

template <class Owner, class T>
void int_property(py::class_<Owner>& cls, const char* name, T Owner::*member) {
  cls.def_property(
      name, [member](const Owner& self) { return self.*member; },
      [member, name](Owner& self, py::handle value) { self.*member = int_from<T>(value, name); });
}

}  // namespace

PYBIND11_MODULE(_core, m) {
  m.doc() = "quantagent C++ core bindings";

  m.def("version", [] { return std::string(qa::version()); }, "Version of the C++ core.");
  m.def("hello", &qa::hello, "Skeleton smoke test for the C++ -> Python path.");

  // --- value types ---------------------------------------------------------------------
  bind_strong<qa::Price, &qa::Price::mantissa>(m, "Price", "A price mantissa (ADR 0005).");
  bind_strong<qa::Qty, &qa::Qty::mantissa>(m, "Qty", "A quantity mantissa (ADR 0005).");
  bind_strong<qa::Timestamp, &qa::Timestamp::ns>(m, "Timestamp",
                                                 "Wall-clock ns since the Unix epoch.");
  bind_strong<qa::MonoTimestamp, &qa::MonoTimestamp::ns>(
      m, "MonoTimestamp", "The monotonic counter in ns (ADR 0012).");

  py::class_<qa::InstrumentId>(m, "InstrumentId")
      .def(py::init([](py::handle value) {
             return qa::InstrumentId(int_from<std::uint32_t>(value, "InstrumentId"));
           }),
           py::arg("value"))
      .def_property_readonly("value", &qa::InstrumentId::value)
      .def("__eq__",
           [](const qa::InstrumentId& self, py::handle other) -> py::object {
             if (!py::isinstance<qa::InstrumentId>(other)) {
               return not_implemented();
             }
             return py::bool_(self == other.cast<const qa::InstrumentId&>());
           })
      .def("__hash__", [](const qa::InstrumentId& self) { return py::hash(py::int_(self.value())); })
      .def("__repr__", [](const qa::InstrumentId& self) {
        return "InstrumentId(" + std::to_string(self.value()) + ")";
      });

  py::enum_<qa::Side>(m, "Side")
      .value("Sell", qa::Side::Sell)
      .value("Unknown", qa::Side::Unknown)
      .value("Buy", qa::Side::Buy);
  py::enum_<qa::EventKind>(m, "EventKind")
      .value("BookSnapshot", qa::EventKind::BookSnapshot)
      .value("BookDelta", qa::EventKind::BookDelta)
      .value("BookCheckSnapshot", qa::EventKind::BookCheckSnapshot)
      .value("Trade", qa::EventKind::Trade)
      .value("StreamStale", qa::EventKind::StreamStale)
      .value("StreamReset", qa::EventKind::StreamReset)
      .value("SnapshotBegin", qa::EventKind::SnapshotBegin)
      .value("SnapshotEnd", qa::EventKind::SnapshotEnd)
      .value("InstrumentUpdate", qa::EventKind::InstrumentUpdate);
  py::enum_<qa::StreamReason>(m, "StreamReason")
      .value("NoReason", qa::StreamReason::NoReason)
      .value("SequenceGap", qa::StreamReason::SequenceGap)
      .value("SequenceRepeat", qa::StreamReason::SequenceRepeat)
      .value("SequenceOutOfOrder", qa::StreamReason::SequenceOutOfOrder)
      .value("ChecksumMismatch", qa::StreamReason::ChecksumMismatch)
      .value("MessageRejected", qa::StreamReason::MessageRejected)
      .value("SessionStart", qa::StreamReason::SessionStart)
      .value("Connect", qa::StreamReason::Connect)
      .value("Disconnect", qa::StreamReason::Disconnect);
  py::enum_<qa::RequestKind>(m, "RequestKind")
      .value("New", qa::RequestKind::New)
      .value("Cancel", qa::RequestKind::Cancel);
  py::enum_<qa::OrderType>(m, "OrderType")
      .value("Limit", qa::OrderType::Limit)
      .value("Market", qa::OrderType::Market);
  py::enum_<qa::TimeInForce>(m, "TimeInForce")
      .value("GoodTillCancel", qa::TimeInForce::GoodTillCancel)
      .value("ImmediateOrCancel", qa::TimeInForce::ImmediateOrCancel)
      .value("PostOnly", qa::TimeInForce::PostOnly);
  py::enum_<qa::ReportKind>(m, "ReportKind")
      .value("Ack", qa::ReportKind::Ack)
      .value("Reject", qa::ReportKind::Reject)
      .value("PartialFill", qa::ReportKind::PartialFill)
      .value("Fill", qa::ReportKind::Fill)
      .value("Cancelled", qa::ReportKind::Cancelled);

  py::class_<qa::InstrumentMeta>(m, "InstrumentMeta")
      .def(py::init([](qa::InstrumentId id, py::handle price_exp, py::handle qty_exp,
                       py::handle price_precision, py::handle qty_precision, qa::Price tick_size,
                       qa::Qty lot_size) {
             const auto price_exponent = exponent_from(price_exp, "price_exp");
             const auto qty_exponent = exponent_from(qty_exp, "qty_exp");
             return qa::InstrumentMeta{
                 id,
                 price_exponent,
                 qty_exponent,
                 precision_from(price_precision, price_exponent, "price_precision"),
                 precision_from(qty_precision, qty_exponent, "qty_precision"),
                 tick_size,
                 lot_size};
           }),
           py::arg("id"), py::arg("price_exp"), py::arg("qty_exp"), py::arg("price_precision"),
           py::arg("qty_precision"), py::arg("tick_size"), py::arg("lot_size"))
      .def_readonly("id", &qa::InstrumentMeta::id)
      .def_readonly("price_exp", &qa::InstrumentMeta::price_exp)
      .def_readonly("qty_exp", &qa::InstrumentMeta::qty_exp)
      .def_readonly("price_precision", &qa::InstrumentMeta::price_precision)
      .def_readonly("qty_precision", &qa::InstrumentMeta::qty_precision)
      .def_readonly("tick_size", &qa::InstrumentMeta::tick_size)
      .def_readonly("lot_size", &qa::InstrumentMeta::lot_size);

  m.def(
      "notional",
      [](qa::Price price, qa::Qty qty) {
        const qa::Int128 value = qa::notional(price, qty);
        return (py::int_(value.hi) << py::int_(64)) | py::int_(value.lo);
      },
      py::arg("price"), py::arg("qty"),
      "The exact notional price x qty, at exponent price_exp + qty_exp, as a Python int.");

  // --- messages ------------------------------------------------------------------------
  using qa::MarketEvent;
  py::class_<MarketEvent> event(
      m, "MarketEvent", "One book level, trade or stream control event (88 bytes, ADR 0020).");
  event.def(py::init<>())
      .def_readwrite("recv_ts", &MarketEvent::recv_ts)
      .def_readwrite("recv_mono", &MarketEvent::recv_mono)
      .def_readwrite("event_ts", &MarketEvent::event_ts)
      .def_readwrite("price", &MarketEvent::price)
      .def_readwrite("qty", &MarketEvent::qty)
      .def_readwrite("instrument", &MarketEvent::instrument)
      .def_readwrite("kind", &MarketEvent::kind)
      .def_readwrite("side", &MarketEvent::side)
      .def_readwrite("reason", &MarketEvent::reason)
      .def("has", [](const MarketEvent& self, py::handle flag) {
        return self.has(int_from<std::uint8_t>(flag, "flag"));
      });
  int_property(event, "seq", &MarketEvent::seq);
  int_property(event, "venue_seq", &MarketEvent::venue_seq);
  int_property(event, "trade_id", &MarketEvent::trade_id);
  int_property(event, "checksum", &MarketEvent::checksum);
  int_property(event, "level_index", &MarketEvent::level_index);
  int_property(event, "depth", &MarketEvent::depth);
  int_property(event, "flags", &MarketEvent::flags);
  int_property(event, "source", &MarketEvent::source);
  event.attr("LAST_IN_MESSAGE") = MarketEvent::kLastInMessage;
  event.attr("HAS_MONO") = MarketEvent::kHasMono;
  event.attr("HAS_VENUE_SEQ") = MarketEvent::kHasVenueSeq;
  event.attr("HAS_CHECKSUM") = MarketEvent::kHasChecksum;
  event.attr("HAS_DEPTH") = MarketEvent::kHasDepth;
  event.attr("HAS_TRADE_ID") = MarketEvent::kHasTradeId;
  event.attr("EVENT_TS_FROM_RECEIPT") = MarketEvent::kEventTsFromReceipt;
  event.attr("KNOWN_FLAGS") = MarketEvent::kKnownFlags;
  event.def("to_bytes", &to_bytes<MarketEvent>)
      .def_static("from_bytes",
                  [](const py::bytes& raw) {
                    const auto value = from_bytes<MarketEvent>(raw);
                    require_at_most(value.kind, qa::EventKind::InstrumentUpdate, "kind");
                    require_side(value.side);
                    require_at_most(value.reason, qa::StreamReason::Disconnect, "reason");
                    require_zero(value.reserved);
                    require_well_formed(value);
                    return value;
                  })
      .def_static(
          "instrument_update",
          [](const qa::InstrumentMeta& meta) {
            MarketEvent made{};
            qa::set_instrument_meta(made, meta);
            return made;
          },
          py::arg("meta"),
          "An InstrumentUpdate carrying `meta` (ADR 0022); every other field is zero.")
      .def(
          "instrument_meta",
          [](const MarketEvent& self) {
            if (self.kind != qa::EventKind::InstrumentUpdate) {
              throw py::value_error(
                  "only an InstrumentUpdate carries instrument metadata, not kind " +
                  std::to_string(static_cast<int>(self.kind)));
            }
            const qa::InstrumentMeta meta = qa::instrument_meta(self);
            require_scales(meta, "the event's");
            return meta;
          },
          "The InstrumentMeta an InstrumentUpdate carries (ADR 0022).")
      .def_static("size", [] { return sizeof(MarketEvent); })
      .def_static("layout", [] {
        return py::dict(py::arg("recv_ts") = py::make_tuple(offsetof(MarketEvent, recv_ts), 8),
                        py::arg("recv_mono") = py::make_tuple(offsetof(MarketEvent, recv_mono), 8),
                        py::arg("event_ts") = py::make_tuple(offsetof(MarketEvent, event_ts), 8),
                        py::arg("seq") = py::make_tuple(offsetof(MarketEvent, seq), 8),
                        py::arg("venue_seq") = py::make_tuple(offsetof(MarketEvent, venue_seq), 8),
                        py::arg("trade_id") = py::make_tuple(offsetof(MarketEvent, trade_id), 8),
                        py::arg("price") = py::make_tuple(offsetof(MarketEvent, price), 8),
                        py::arg("qty") = py::make_tuple(offsetof(MarketEvent, qty), 8),
                        py::arg("instrument") =
                            py::make_tuple(offsetof(MarketEvent, instrument),
                                           sizeof(MarketEvent::instrument)),
                        py::arg("checksum") = py::make_tuple(offsetof(MarketEvent, checksum),
                                                             sizeof(MarketEvent::checksum)),
                        py::arg("level_index") =
                            py::make_tuple(offsetof(MarketEvent, level_index),
                                           sizeof(MarketEvent::level_index)),
                        py::arg("depth") = py::make_tuple(offsetof(MarketEvent, depth),
                                                          sizeof(MarketEvent::depth)),
                        py::arg("kind") = py::make_tuple(offsetof(MarketEvent, kind),
                                                         sizeof(MarketEvent::kind)),
                        py::arg("side") = py::make_tuple(offsetof(MarketEvent, side),
                                                         sizeof(MarketEvent::side)),
                        py::arg("flags") = py::make_tuple(offsetof(MarketEvent, flags),
                                                          sizeof(MarketEvent::flags)),
                        py::arg("reason") = py::make_tuple(offsetof(MarketEvent, reason),
                                                           sizeof(MarketEvent::reason)),
                        py::arg("source") = py::make_tuple(offsetof(MarketEvent, source),
                                                           sizeof(MarketEvent::source)));
      });

  using qa::OrderRequest;
  py::class_<OrderRequest> request(m, "OrderRequest", "An order request (64 bytes).");
  request.def(py::init<>())
      .def_readwrite("price", &OrderRequest::price)
      .def_readwrite("qty", &OrderRequest::qty)
      .def_readwrite("created_ts", &OrderRequest::created_ts)
      .def_readwrite("created_mono", &OrderRequest::created_mono)
      .def_readwrite("instrument", &OrderRequest::instrument)
      .def_readwrite("kind", &OrderRequest::kind)
      .def_readwrite("side", &OrderRequest::side)
      .def_readwrite("type", &OrderRequest::type)
      .def_readwrite("time_in_force", &OrderRequest::time_in_force);
  int_property(request, "client_order_id", &OrderRequest::client_order_id);
  int_property(request, "target_order_id", &OrderRequest::target_order_id);
  request.def("to_bytes", &to_bytes<OrderRequest>)
      .def_static("from_bytes",
                  [](const py::bytes& raw) {
                    const auto value = from_bytes<OrderRequest>(raw);
                    require_at_most(value.kind, qa::RequestKind::Cancel, "kind");
                    require_side(value.side);
                    require_at_most(value.type, qa::OrderType::Market, "type");
                    require_at_most(value.time_in_force, qa::TimeInForce::PostOnly,
                                    "time_in_force");
                    require_zero(value.reserved);
                    return value;
                  })
      .def_static("size", [] { return sizeof(OrderRequest); })
      .def_static("layout", [] {
        py::dict layout;
        layout["client_order_id"] = py::make_tuple(offsetof(OrderRequest, client_order_id), 8);
        layout["target_order_id"] = py::make_tuple(offsetof(OrderRequest, target_order_id), 8);
        layout["price"] = py::make_tuple(offsetof(OrderRequest, price), 8);
        layout["qty"] = py::make_tuple(offsetof(OrderRequest, qty), 8);
        layout["created_ts"] = py::make_tuple(offsetof(OrderRequest, created_ts), 8);
        layout["created_mono"] = py::make_tuple(offsetof(OrderRequest, created_mono), 8);
        layout["instrument"] =
            py::make_tuple(offsetof(OrderRequest, instrument), sizeof(OrderRequest::instrument));
        layout["kind"] = py::make_tuple(offsetof(OrderRequest, kind), sizeof(OrderRequest::kind));
        layout["side"] = py::make_tuple(offsetof(OrderRequest, side), sizeof(OrderRequest::side));
        layout["type"] = py::make_tuple(offsetof(OrderRequest, type), sizeof(OrderRequest::type));
        layout["time_in_force"] = py::make_tuple(offsetof(OrderRequest, time_in_force),
                                                 sizeof(OrderRequest::time_in_force));
        return layout;
      });

  using qa::ExecutionReport;
  py::class_<ExecutionReport> report(m, "ExecutionReport", "An execution report (80 bytes).");
  report.def(py::init<>())
      .def_readwrite("last_price", &ExecutionReport::last_price)
      .def_readwrite("last_qty", &ExecutionReport::last_qty)
      .def_readwrite("leaves_qty", &ExecutionReport::leaves_qty)
      .def_readwrite("event_ts", &ExecutionReport::event_ts)
      .def_readwrite("recv_ts", &ExecutionReport::recv_ts)
      .def_readwrite("recv_mono", &ExecutionReport::recv_mono)
      .def_readwrite("instrument", &ExecutionReport::instrument)
      .def_readwrite("kind", &ExecutionReport::kind)
      .def_readwrite("side", &ExecutionReport::side);
  int_property(report, "client_order_id", &ExecutionReport::client_order_id);
  int_property(report, "exec_id", &ExecutionReport::exec_id);
  int_property(report, "reject_code", &ExecutionReport::reject_code);
  report.def("to_bytes", &to_bytes<ExecutionReport>)
      .def_static("from_bytes",
                  [](const py::bytes& raw) {
                    const auto value = from_bytes<ExecutionReport>(raw);
                    require_at_most(value.kind, qa::ReportKind::Cancelled, "kind");
                    require_side(value.side);
                    require_zero(value.reserved);
                    return value;
                  })
      .def_static("size", [] { return sizeof(ExecutionReport); })
      .def_static("layout", [] {
        py::dict layout;
        layout["client_order_id"] = py::make_tuple(offsetof(ExecutionReport, client_order_id), 8);
        layout["exec_id"] = py::make_tuple(offsetof(ExecutionReport, exec_id), 8);
        layout["last_price"] = py::make_tuple(offsetof(ExecutionReport, last_price), 8);
        layout["last_qty"] = py::make_tuple(offsetof(ExecutionReport, last_qty), 8);
        layout["leaves_qty"] = py::make_tuple(offsetof(ExecutionReport, leaves_qty), 8);
        layout["event_ts"] = py::make_tuple(offsetof(ExecutionReport, event_ts), 8);
        layout["recv_ts"] = py::make_tuple(offsetof(ExecutionReport, recv_ts), 8);
        layout["recv_mono"] = py::make_tuple(offsetof(ExecutionReport, recv_mono), 8);
        layout["instrument"] = py::make_tuple(offsetof(ExecutionReport, instrument),
                                              sizeof(ExecutionReport::instrument));
        layout["reject_code"] = py::make_tuple(offsetof(ExecutionReport, reject_code),
                                               sizeof(ExecutionReport::reject_code));
        layout["kind"] =
            py::make_tuple(offsetof(ExecutionReport, kind), sizeof(ExecutionReport::kind));
        layout["side"] =
            py::make_tuple(offsetof(ExecutionReport, side), sizeof(ExecutionReport::side));
        return layout;
      });

  // --- the order book (ADR 0022) ------------------------------------------------------------------
  py::enum_<qa::BookCheck>(m, "BookCheck")
      .value("KrakenChecksum", qa::BookCheck::KrakenChecksum)
      .value("CoinbaseSequence", qa::BookCheck::CoinbaseSequence);
  py::enum_<qa::StaleReason>(m, "StaleReason")
      .value("NotStale", qa::StaleReason::NotStale)
      .value("SequenceGap", qa::StaleReason::SequenceGap)
      .value("SequenceRepeat", qa::StaleReason::SequenceRepeat)
      .value("SequenceOutOfOrder", qa::StaleReason::SequenceOutOfOrder)
      .value("ChecksumMismatch", qa::StaleReason::ChecksumMismatch)
      .value("MessageRejected", qa::StaleReason::MessageRejected)
      .value("DeltaBeforeSnapshot", qa::StaleReason::DeltaBeforeSnapshot)
      .value("IncompleteMessage", qa::StaleReason::IncompleteMessage)
      .value("CapacityExceeded", qa::StaleReason::CapacityExceeded)
      .value("InvalidEvent", qa::StaleReason::InvalidEvent);
  py::enum_<qa::BookEffect>(m, "BookEffect")
      .value("Pending", qa::BookEffect::Pending)
      .value("Applied", qa::BookEffect::Applied)
      .value("Recovered", qa::BookEffect::Recovered)
      .value("Skipped", qa::BookEffect::Skipped)
      .value("WentStale", qa::BookEffect::WentStale)
      .value("Reset", qa::BookEffect::Reset)
      .value("Instrument", qa::BookEffect::Instrument)
      .value("Ignored", qa::BookEffect::Ignored);
  py::class_<qa::BookResult>(m, "BookResult", "What one event did to an OrderBook.")
      .def_readonly("effect", &qa::BookResult::effect)
      .def_readonly("reason", &qa::BookResult::reason)
      .def("__eq__",
           [](const qa::BookResult& self, py::handle other) -> py::object {
             if (!py::isinstance<qa::BookResult>(other)) {
               return not_implemented();
             }
             return py::bool_(self == other.cast<const qa::BookResult&>());
           })
      .def("__repr__", [](const qa::BookResult& self) {
        return "BookResult(" + enum_name(self.effect) + ", " + enum_name(self.reason) + ")";
      });

  using qa::OrderBook;
  py::class_<OrderBook> book(m, "OrderBook",
                             "One instrument's L2 order book, given MarketEvents one at a time "
                             "(ADR 0022). Prices and quantities are integer mantissas.");
  book.def(py::init([](const qa::InstrumentMeta& instrument, qa::BookCheck check,
                       py::handle capacity) {
             return OrderBook(instrument, check, int_from<std::uint32_t>(capacity, "capacity"));
           }),
           py::arg("instrument"), py::arg("check"),
           py::arg("capacity") = OrderBook::kDefaultCapacity)
      .def("apply", &OrderBook::apply, py::arg("event"))
      .def_property_readonly("is_stale", &OrderBook::is_stale)
      .def_property_readonly("has_book", &OrderBook::has_book)
      .def_property_readonly("stale_reason", &OrderBook::stale_reason)
      .def_property_readonly("instrument",
                             [](const OrderBook& self) { return self.instrument(); })
      .def_property_readonly("check", &OrderBook::check)
      .def_property_readonly("capacity", &OrderBook::capacity)
      .def_property_readonly(
          "bound_source",
          [](const OrderBook& self) -> py::object {
            const std::optional<std::uint32_t> source = self.bound_source();
            if (!source) {
              return py::none();
            }
            return py::int_(*source);
          },
          "The source the book is bound to (ADR 0023), or None.")
      .def_property_readonly("other_source_updates", &OrderBook::other_source_updates,
                             "How many updates of another source were ignored (ADR 0023).")
      .def_property_readonly(
          "other_source_controls", &OrderBook::other_source_controls,
          "How many StreamStale and StreamReset events of another source were ignored (ADR 0023).")
      .def("stale_count", &OrderBook::stale_count, py::arg("reason"))
      .def(
          "top",
          [](const OrderBook& self, qa::Side side, py::handle count) {
            const auto wanted = int_from<std::uint32_t>(count, "count");
            std::vector<qa::BookLevel> levels(std::min(wanted, self.size(side)));
            const std::uint32_t found = self.top(side, levels);
            py::list out;
            for (std::uint32_t i = 0; i < found; ++i) {
              out.append(py::make_tuple(levels[i].price.mantissa(), levels[i].qty.mantissa()));
            }
            return out;
          },
          py::arg("side"), py::arg("count"),
          "The best `count` levels of one side, best first, as (price, quantity) mantissas.")
      .def("best_bid", [](const OrderBook& self) { return level_or_none(self.best_bid()); })
      .def("best_ask", [](const OrderBook& self) { return level_or_none(self.best_ask()); })
      .def("size", &OrderBook::size, py::arg("side"))
      .def(
          "quantity_at",
          [](const OrderBook& self, qa::Side side, py::handle price) -> py::object {
            const auto qty =
                self.quantity_at(side, qa::Price(int_from<std::int64_t>(price, "price")));
            if (!qty) {
              return py::none();
            }
            return py::int_(qty->mantissa());
          },
          py::arg("side"), py::arg("price"))
      .def("checksum", &OrderBook::checksum)
      .def_static(
          "capacity_for_depth",
          [](py::handle depth) {
            return OrderBook::capacity_for_depth(int_from<std::uint16_t>(depth, "depth"));
          },
          py::arg("depth"));
  book.attr("DEFAULT_CAPACITY") = OrderBook::kDefaultCapacity;
  book.attr("MAX_CAPACITY") = OrderBook::kMaxCapacity;

  // --- clocks --------------------------------------------------------------------------------
  py::class_<qa::SystemClock>(m, "SystemClock")
      .def(py::init<>())
      .def("wall", &qa::SystemClock::wall)
      .def("mono", &qa::SystemClock::mono);
  py::class_<qa::ManualClock>(m, "ManualClock")
      .def(py::init<qa::Timestamp, qa::MonoTimestamp>(), py::arg("wall"), py::arg("mono"))
      .def("wall", &qa::ManualClock::wall)
      .def("mono", &qa::ManualClock::mono)
      .def("set", &qa::ManualClock::set, py::arg("wall"), py::arg("mono"))
      .def(
          "advance",
          [](qa::ManualClock& self, py::handle ns) {
            self.advance(int_from<std::int64_t>(ns, "ns"));
          },
          py::arg("ns"))
      .def(
          "step_wall",
          [](qa::ManualClock& self, py::handle ns) {
            self.step_wall(int_from<std::int64_t>(ns, "ns"));
          },
          py::arg("ns"));
}
