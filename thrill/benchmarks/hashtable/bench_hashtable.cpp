/*******************************************************************************
 * benchmarks/hashtable/bench_hashtable.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/stats_timer.hpp>
#include <thrill/core/reduce_by_hash_post_phase.hpp>
#include <thrill/data/block_writer.hpp>
#include <thrill/data/discard_sink.hpp>
#include <thrill/data/file.hpp>
#include <tlx/cmdline_parser.hpp>

#include <algorithm>
#include <cmath>
#include <functional>
#include <iostream>
#include <limits>
#include <numeric>
#include <random>
#include <string>
#include <utility>
#include <vector>

using Key = uint64_t;
using KeyPair = std::pair<uint64_t, uint64_t>;

using namespace thrill; // NOLINT

std::string title;
uint64_t size = 64 * 1024 * 1024;
unsigned int workers = 100;

uint64_t item_range = std::numeric_limits<Key>::max();

template <core::ReduceTableImpl table_impl>
void RunBenchmark(api::Context& ctx, core::DefaultReduceConfig& base_config) {

    auto key_ex = [](const Key& in) { return in; };

    auto red_fn = [](const Key& in1, const Key& in2) {
                      (void)in2;
                      return in1;
                  };

    auto emit_fn = [](const Key&) { };

    uint64_t num_items = size / sizeof(KeyPair);

    std::default_random_engine rng(std::random_device { } ());
    std::uniform_int_distribution<Key> dist(1, item_range);

    core::DefaultReduceConfigSelect<table_impl> config;
    config.limit_partition_fill_rate_ = base_config.limit_partition_fill_rate_;
    config.bucket_rate_ = base_config.bucket_rate_;

    core::ReduceByHashPostPhase<
        Key, Key, Key,
        decltype(key_ex), decltype(red_fn), decltype(emit_fn),
        /* VolatileKey */ false,
        core::DefaultReduceConfigSelect<table_impl> >
    phase(ctx, 0, key_ex, red_fn, emit_fn,
          config);

    common::StatsTimerStart timer;

    for (uint64_t i = 0; i < num_items; i++)
        phase.Insert(dist(rng));

    phase.PushData(/* consume */ true);

    timer.Stop();

    std::cout
        << "RESULT"
        << " benchmark=" << title
        << " size=" << size
        << " workers=" << workers
        << " max_partition_fill_rate=" << config.limit_partition_fill_rate()
        << " bucket_rate=" << config.bucket_rate()
        << " time=" << timer
        << std::endl;
}

int main(int argc, char* argv[]) {

    tlx::CmdlineParser clp;

    core::DefaultReduceConfig config;

    std::string hashtable;

    clp.add_bytes('s', "size", "S", size,
                  "Set amount of bytes to be inserted, default = 64 MiB");

    clp.add_string('t', "title", "T", title,
                   "Load in byte to be inserted");

    clp.add_string('h', "hash-table", "H", hashtable,
                   "Set hashtable: probing or bucket");

    clp.add_unsigned('w', "workers", "W", workers,
                     "Open hashtable with W workers, default = 1.");

    clp.add_double('f', "fill_rate", "F",
                   config.limit_partition_fill_rate_,
                   "set limit_partition_fill_rate, default = 0.5.");

    clp.add_double('b', "bucket_rate", "B",
                   config.bucket_rate_,
                   "set bucket_rate, default = 0.5.");

    clp.add_bytes('r', "range", "N",
                  item_range,
                  "set upper bound on item values, default = UINT_MAX.");

    if (!clp.process(argc, argv)) {
        return -1;
    }

    api::RunLocalSameThread(
        [&](api::Context& ctx) {
            if (hashtable == "bucket")
                return RunBenchmark<core::ReduceTableImpl::BUCKET>(ctx, config);
            else
                return RunBenchmark<core::ReduceTableImpl::PROBING>(ctx, config);
        });

    return 0;
}

/******************************************************************************/
