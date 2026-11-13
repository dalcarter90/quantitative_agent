#include "qa/clock.hpp"
#include "qa/events.hpp"
#include "qa/notional.hpp"
#include "qa/types.hpp"
#include "qa/version.hpp"

#include <pybind11/pybind11.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <type_traits>

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
      .value("Trade", qa::EventKind::Trade);
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
                       qa::Price tick_size, qa::Qty lot_size) {
             return qa::InstrumentMeta{id, int_from<std::int8_t>(price_exp, "price_exp"),
                                       int_from<std::int8_t>(qty_exp, "qty_exp"), tick_size,
                                       lot_size};
           }),
           py::arg("id"), py::arg("price_exp"), py::arg("qty_exp"), py::arg("tick_size"),
           py::arg("lot_size"))
      .def_readonly("id", &qa::InstrumentMeta::id)
      .def_readonly("price_exp", &qa::InstrumentMeta::price_exp)
      .def_readonly("qty_exp", &qa::InstrumentMeta::qty_exp)
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
  py::class_<MarketEvent> event(m, "MarketEvent", "One book level or trade (88 bytes).");
  event.def(py::init<>())
      .def_readwrite("recv_ts", &MarketEvent::recv_ts)
      .def_readwrite("recv_mono", &MarketEvent::recv_mono)
      .def_readwrite("event_ts", &MarketEvent::event_ts)
      .def_readwrite("price", &MarketEvent::price)
      .def_readwrite("qty", &MarketEvent::qty)
      .def_readwrite("instrument", &MarketEvent::instrument)
      .def_readwrite("kind", &MarketEvent::kind)
      .def_readwrite("side", &MarketEvent::side)
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
  event.attr("LAST_IN_MESSAGE") = MarketEvent::kLastInMessage;
  event.attr("HAS_MONO") = MarketEvent::kHasMono;
  event.attr("HAS_VENUE_SEQ") = MarketEvent::kHasVenueSeq;
  event.attr("HAS_CHECKSUM") = MarketEvent::kHasChecksum;
  event.attr("HAS_DEPTH") = MarketEvent::kHasDepth;
  event.attr("HAS_TRADE_ID") = MarketEvent::kHasTradeId;
  event.attr("EVENT_TS_FROM_RECEIPT") = MarketEvent::kEventTsFromReceipt;
  event.def("to_bytes", &to_bytes<MarketEvent>)
      .def_static("from_bytes",
                  [](const py::bytes& raw) {
                    const auto value = from_bytes<MarketEvent>(raw);
                    require_at_most(value.kind, qa::EventKind::Trade, "kind");
                    require_side(value.side);
                    require_zero(value.reserved);
                    return value;
                  })
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
                                                          sizeof(MarketEvent::flags)));
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
