#include <pybind11/pybind11.h>
#include <pybind11/iostream.h>
#include <string>
#include <iostream>
#include <arrow/api.h>
#include <re2/re2.h>
#include "Hexdump.hpp"

using namespace std;

#define STRINGIFY(x) #x
#define MACRO_STRINGIFY(x) STRINGIFY(x)

int add(int i, int j) {
    return i + j;
}

void make_record_batch_with_buf_addrs(std::shared_ptr<arrow::Schema> schema, int num_rows,
                                        long int in_buf_addrs[], long int in_buf_sizes[],
                                        int in_bufs_len,
                                        std::shared_ptr<arrow::RecordBatch> *batch) {
    std::vector<std::shared_ptr<arrow::ArrayData>> columns;
    auto num_fields = schema->num_fields();
    int buf_idx = 0;
    int sz_idx = 0;

    for (int i = 0; i < num_fields; i++) {
        auto field = schema->field(i);
        std::vector<std::shared_ptr<arrow::Buffer>> buffers;

        bool nullable = field->nullable();

        if (buf_idx >= in_bufs_len) {
            std::cout << "insufficient number of in_buf_addrs" << std::endl;
        }

        if (nullable) {
            long int validity_addr = in_buf_addrs[buf_idx++];
            long int validity_size = in_buf_sizes[sz_idx++];
            auto validity = std::shared_ptr<arrow::Buffer>(
                    new arrow::Buffer(reinterpret_cast<uint8_t *>(validity_addr), validity_size));
            buffers.push_back(validity);
        } else { //if Field is not nullable ignore validity buffer
            buffers.push_back(nullptr);
        }

        if (buf_idx >= in_bufs_len) {
            std::cout << "insufficient number of in_buf_addrs" << std::endl;
        }
        long int value_addr = in_buf_addrs[buf_idx++];
        long int value_size = in_buf_sizes[sz_idx++];
        auto data = std::shared_ptr<arrow::Buffer>(
                new arrow::Buffer(reinterpret_cast<uint8_t *>(value_addr), value_size));
        buffers.push_back(data);

        if (arrow::is_binary_like(field->type()->id())) {
            if (buf_idx >= in_bufs_len) {
                std::cout << "insufficient number of in_buf_addrs" << std::endl;
            }

            // add offsets buffer for variable-len fields.
            long int offsets_addr = in_buf_addrs[buf_idx++];
            long int offsets_size = in_buf_sizes[sz_idx++];
            auto offsets = std::shared_ptr<arrow::Buffer>(
                    new arrow::Buffer(reinterpret_cast<uint8_t *>(offsets_addr), offsets_size));
            buffers.push_back(offsets);
        }

        std::shared_ptr<arrow::ArrayData> array_data;
        if (nullable) {
            array_data = arrow::ArrayData::Make(field->type(), num_rows, std::move(buffers));
        } else {
            array_data = arrow::ArrayData::Make(field->type(), num_rows, std::move(buffers), 0);
        }

        columns.push_back(array_data);
    }
    *batch = arrow::RecordBatch::Make(schema, num_rows, columns);
}

void dumpBuffer(long int addr, int size_in_bytes) {
    // Redirect std::ostream to python output
    pybind11::scoped_ostream_redirect stream(
        std::cout,
        pybind11::module_::import("sys").attr("stdout")
    );

    void* addr_pointer = reinterpret_cast<void *>(addr);
    std::cout << Hexdump(addr_pointer, size_in_bytes) << std::endl;
}

void re2Eval(int number_of_records, const std::string &regex, long int offset_addr, long int value_addr, int offset_size, int value_size, long int out_addr, int out_size) {

    // Redirect std::ostream to python output
    pybind11::scoped_ostream_redirect stream(
        std::cout,
        pybind11::module_::import("sys").attr("stdout")
    );

    long int in_buf_addrs[] = {offset_addr, value_addr};
    long int in_buf_sizes[] = {offset_size, value_size};

    std::shared_ptr<arrow::Field> field_a;
    std::shared_ptr<arrow::Schema> schema;
    field_a = arrow::field("string", arrow::utf8(), false);
    schema = arrow::schema({field_a});
    std::shared_ptr<arrow::RecordBatch> inBatch;
    make_record_batch_with_buf_addrs(schema, number_of_records, in_buf_addrs, in_buf_sizes, 2, &inBatch);

    auto strings = std::static_pointer_cast<arrow::StringArray>(inBatch->column(0));

    // The output SV is an array of int32's so we can access it using a simple pointer
    auto out_values = reinterpret_cast<uint8_t *>(out_addr);

    RE2 re(regex);

    for (int sv_byte = 0; sv_byte < out_size; sv_byte++) {
        for (int bit = 0; bit < 8; bit++) {
            int record_index = sv_byte * 8 + bit;
            if (RE2::FullMatch(strings->GetString(record_index), re)) {
                out_values[sv_byte] |= 1UL << bit;
            }
        }
    }
}

namespace py = pybind11;

PYBIND11_MODULE(dask_native, m) {
    m.doc() = R"pbdoc(
        Pybind11 example plugin
        -----------------------
        .. currentmodule:: cmake_example
        .. autosummary::
           :toctree: _generate
           add
           subtract
    )pbdoc";

    m.def("add", &add, R"pbdoc(
        Add two numbers
        Some other explanation about the add function.
    )pbdoc");

    m.def("utf8_test",
        [](const std::string &s) {
            py::scoped_ostream_redirect stream(
                std::cout,                               // std::ostream&
                py::module_::import("sys").attr("stdout") // Python output
            );
            cout << "utf-8 is icing on the cake.\n";
            cout << s;
        }
    );

    m.def("re2Eval", &re2Eval, R"pbdoc(
        String matcher
        Do google RE2 eval of string match op on recordbatch.
    )pbdoc");

    m.def("dumpBuffer", &dumpBuffer, R"pbdoc(
        Dump buffer
        Makes a hexdump of the buffer content and prints to console.
    )pbdoc");

    m.def("subtract", [](int i, int j) { return i - j; }, R"pbdoc(
        Subtract two numbers
        Some other explanation about the subtract function.
    )pbdoc");

#ifdef VERSION_INFO
    m.attr("__version__") = MACRO_STRINGIFY(VERSION_INFO);
#else
    m.attr("__version__") = "dev";
#endif
}