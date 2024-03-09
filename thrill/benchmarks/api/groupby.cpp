/*******************************************************************************
 * benchmarks/api/groupby.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Huyen Chau Nguyen <hello@chau-nguyen.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/cache.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/api/group_by_key.hpp>
#include <thrill/api/group_to_index.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stats_timer.hpp>
#include <tlx/cmdline_parser.hpp>

#include <algorithm>
#include <iostream>
#include <iterator>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

// using thrill::DIARef;
using thrill::Context;

using namespace thrill; // NOLINT

int main(int argc, char* argv[]) {
    tlx::CmdlineParser clp;

    std::string input;
    clp.add_param_string("input", input,
                         "input file pattern");

    if (!clp.process(argc, argv)) {
        return -1;
    }

    clp.print_result();

    auto start_func = [&input](api::Context& ctx) {

                          auto modulo_keyfn = [](size_t in) { return (in % 100); };

                          auto median_fn = [](auto& r, std::size_t) {
                                               std::vector<std::size_t> all;
                                               while (r.HasNext()) {
                                                   all.push_back(r.Next());
                                               }
                                               std::sort(std::begin(all), std::end(all));
                                               return all[all.size() / 2 - 1];
                                           };

                          auto in = api::ReadBinary<size_t>(ctx, input).Cache();
                          auto res1 = in.Size();

                          // group by to compute median
                          thrill::common::StatsTimerStart timer;
                          auto res2 = in.GroupByKey<size_t>(modulo_keyfn, median_fn).Size();
                          timer.Stop();

                          LOG1 // << "\n"
                              << "RESULT"
                              << " name=total"
                              << " time=" << timer
                              << " filename=" << input
                              << " sanity1=" << res1
                              << " sanity2=" << res2;
                      };
    for (size_t i = 0; i < 4; ++i) {
        api::Run(start_func);
    }

    return api::Run(start_func);
}

/******************************************************************************/
