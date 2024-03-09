/*******************************************************************************
 * examples/word_count/word_count_run.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) CI0AIPCI Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) CI0AIUBI Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-CI license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/cache.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/write_lines.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/common/string.hpp>
#include <tlx/cmdline_parser.hpp>
#include <thrill/api/inter_map_2d.hpp>
#include <thrill/api/rebalance.hpp>
#include <thrill/api/sample.hpp>
#include <thrill/api/all_gather.hpp>
#include <thrill/common/ndarray.hpp>
#include <algorithm>
#include <random>
#include <string>
#include <utility>
#include <vector>
#include <mpi.h>
#include <ctime>

#define BI 0
#define AI 1
#define CI 2
#define PBI 3
#define PAI 4
#define PCI 5
#define UBI 6
#define UAI 7
#define UCI 8
#define PI 9
#define UI 10
#define B 11
#define X 12


using namespace thrill;               // NOLINT


/******************************************************************************/
// Run methods

static void RunGaBP(
    api::Context& ctx, size_t y_size, std::vector<std::string>& input_filelist, const std::string& output) {
    ctx.enable_consume();

    common::StatsTimerStart timer;

    auto lines = ReadLines(ctx, input_filelist);

    auto numbers = lines.template FlatMap<double>(
        [lines](const std::string& line, auto emit) -> void{
            tlx::split_view(' ', line, [&](const tlx::string_view& sv){
                if(sv.size() == 0) return;
                emit((double)atof(sv.to_string().c_str()));
            });
        });
 

// bi,ai,ci,Pbi,Pai,Pci,Ubi,Uai,Uci,Pi,Ui,b,x

    auto nums = numbers.InterMap2D([y_size](std::vector<double> values){
 
        std::vector<double> results;

        int i,j;

        double tmp_ui_before[values.size()/y_size];
        double tmp_ui_after[values.size()/y_size];
        double tmp_pi_before[values.size()/y_size];
        double tmp_pi_after[values.size()/y_size];

        for(i=1;i<values.size()/y_size-1;i++){
            tmp_ui_before[i] = values[i*y_size+B];
            tmp_pi_before[i] = values[i*y_size+AI];

            if(values[i*y_size+CI] != 0){
                tmp_ui_before[i] = tmp_ui_before[i] + values[(i+1)*y_size+PBI] * values[(i+1)*y_size+UBI];
                tmp_pi_before[i] = tmp_pi_before[i] + values[(i+1)*y_size+PBI];
            }

            if(values[i*y_size] != 0){
                tmp_ui_before[i] = tmp_ui_before[i] + values[(i-1)*y_size+PCI] * values[(i-1)*y_size+UCI];
                tmp_pi_before[i] = tmp_pi_before[i] + values[(i-1)*y_size+PCI];
            }
        }


        if(values[BI] == 0 && values[AI] == 0 & values[CI] == 0){
            for(j=0;j<y_size;j++){
                results.push_back(values[j]);
            }

        }

        for(i=1;i<values.size()/y_size-1;i++){
            values[i*y_size+PI] = values[i*y_size+AI];
            values[i*y_size+PAI] = values[i*y_size+AI];
            values[i*y_size+UI] = values[i*y_size+B];
            values[i*y_size+UAI] = values[i*y_size+B]/values[i*y_size+AI];           

            if(values[i*y_size+BI] != 0){
                values[i*y_size+PI] = values[i*y_size+PI] + values[(i-1)*y_size+PCI];
                values[i*y_size+UI] = values[i*y_size+UI] + values[(i-1)*y_size+UCI] * values[(i-1)*y_size+PCI];
            }

            if(values[i*y_size+CI] != 0){
                values[i*y_size+PI] = values[i*y_size+PI] + values[(i+1)*y_size+PBI];
                values[i*y_size+UI] = values[i*y_size+UI] + values[(i+1)*y_size+UBI] * values[(i+1)*y_size+PBI];
            }

            if(values[i*y_size+PI] == 0) values[i*y_size+PI] = 0.00001;
            values[i*y_size+UI] = values[i*y_size+UI] / values[i*y_size+PI];
        }

        double tmp;
        double err = 0;
        for(i=1;i<values.size()/y_size-1;i++){
            if(values[i*y_size+BI] != 0){

                tmp = (values[i*y_size+PI] - values[(i-1)*y_size+PCI]);
                if(tmp == 0)tmp = 0.00001;
                values[i*y_size+PBI] = -1 * values[i*y_size+BI] * values[i*y_size+BI] / tmp;
                values[i*y_size+UBI] = (values[i*y_size+PI] * values[i*y_size+UI] - values[(i-1)*y_size+PCI] * values[(i-1)*y_size+UCI]) / values[i*y_size+BI];
            }

            if(values[i*y_size+CI] !=0){
                tmp = (values[i*y_size+PI] - values[(i+1)*y_size+PBI]);
                if(tmp == 0) tmp = 0.00001;
                values[i*y_size+PCI] = -1 * values[i*y_size+CI] * values[i*y_size+CI] / tmp;

                values[i*y_size+UCI] = (values[i*y_size+PI] * values[i*y_size+UI] - values[(i+1)*y_size+PBI] * values[(i+1)*y_size+UBI]) / values[i*y_size+CI];

            }

        }

        for(i=1; i<values.size()/y_size-1;i++){
            tmp_ui_after[i] = values[i*y_size+B];
            tmp_pi_after[i] = values[i*y_size+AI];

            if(values[i*y_size+CI] != 0){
                tmp_ui_after[i] = tmp_ui_after[i] + values[(i+1)*y_size+PBI] * values[(i+1)*y_size+UBI];
                tmp_pi_after[i] = tmp_pi_after[i] + values[(i+1)*y_size+PBI];
            }

            if(values[i*y_size] != 0){
                tmp_ui_after[i] = tmp_ui_after[i] + values[(i-1)*y_size+PCI] * values[(i-1)*y_size+UCI];
                tmp_pi_after[i] = tmp_pi_after[i] + values[(i-1)*y_size+PCI];
            }

            double x_before = tmp_ui_before[i] / tmp_pi_before[i];
            double x_after = tmp_ui_after[i] / tmp_pi_after[i];
            if(x_after > x_before){
                err += x_after - x_before;
            }else{
                err += x_before - x_after;
            }



            for(j=0;j<y_size-1;j++){
                results.push_back(values[i*y_size+j]);
            }
            if(i==1 || i==values.size()/y_size-2){
                results.push_back(err);
            }else{
                results.push_back(0);
            }
 

        }

        if(values[values.size()-y_size+BI] == 0 && values[values.size()-y_size+AI] == 0 && values[values.size()-y_size+CI] == 0){
            for(j=0;j<y_size;j++){
                results.push_back(values[values.size()-y_size + j]);
            }

        }
                
        return results;
    },y_size,1,1);

    int iter = 0;
    while(true){
        nums = nums.InterMap2D([y_size,iter](std::vector<double> values) {
        long start_time, end_time;
        start_time = clock();
        std::vector<double> results;
        int k;
        int i,j;
        double err = 0;
        err = values[X] + values[y_size+X] + values[values.size()-1];
        if(iter % 1000 == 0){
            std::cout << "iter " << iter << " done. err:" << err << std::endl;
        }

 /*       if(err < 0.005){

            if(values[BI] == 0 && values[AI] == 0 & values[CI] == 0){
                for(j=0;j<y_size;j++){
                    results.push_back(values[j]);
                }
            }


            for(i=1;i<values.size()/y_size-1;i++){
                for(j=0;j<y_size;j++){
                    results.push_back(values[i*y_size+j]);
                }
            }

            if(values[values.size()-y_size+BI] == 0 && values[values.size()-y_size+AI] == 0 && values[values.size()-y_size+CI] == 0){
                for(j=0;j<y_size;j++){
                    results.push_back(values[values.size()-y_size + j]);
                }
            }

            return results;

        }*/

        double tmp_ui_before[values.size()/y_size];
        double tmp_ui_after[values.size()/y_size];
        double tmp_pi_before[values.size()/y_size];
        double tmp_pi_after[values.size()/y_size];

        for(i=1;i<values.size()/y_size-1;i++){
            tmp_ui_before[i] = values[i*y_size+B];
            tmp_pi_before[i] = values[i*y_size+AI];

            if(values[i*y_size+CI] != 0){
                tmp_ui_before[i] = tmp_ui_before[i] + values[(i+1)*y_size+PBI] * values[(i+1)*y_size+UBI];
                tmp_pi_before[i] = tmp_pi_before[i] + values[(i+1)*y_size+PBI];
            }

            if(values[i*y_size] != 0){
                tmp_ui_before[i] = tmp_ui_before[i] + values[(i-1)*y_size+PCI] * values[(i-1)*y_size+UCI];
                tmp_pi_before[i] = tmp_pi_before[i] + values[(i-1)*y_size+PCI];
            }
        }


        if(values[BI] == 0 && values[AI] == 0 & values[CI] == 0){
            for(j=0;j<y_size;j++){
                results.push_back(values[j]);          
            }
        }

        for(i=1;i<values.size()/y_size-1;i++){
            values[i*y_size+PI] = values[i*y_size+AI];
            values[i*y_size+PAI] = values[i*y_size+AI];
            values[i*y_size+UI] = values[i*y_size+B];
            values[i*y_size+UAI] = values[i*y_size+B]/values[i*y_size+AI];           

            if(values[i*y_size+BI] != 0){
                values[i*y_size+PI] = values[i*y_size+PI] + values[(i-1)*y_size+PCI];
                values[i*y_size+UI] = values[i*y_size+UI] + values[(i-1)*y_size+UCI] * values[(i-1)*y_size+PCI];
            }

            if(values[i*y_size+CI] != 0){
                values[i*y_size+PI] = values[i*y_size+PI] + values[(i+1)*y_size+PBI];
                values[i*y_size+UI] = values[i*y_size+UI] + values[(i+1)*y_size+UBI] * values[(i+1)*y_size+PBI];
            }

            if(values[i*y_size+PI] == 0) values[i*y_size+PI] = 0.00001;
            values[i*y_size+UI] = values[i*y_size+UI] / values[i*y_size+PI];
        }

        double tmp;
        err = 0;
        for(i=1;i<values.size()/y_size-1;i++){
            if(values[i*y_size+BI] != 0){
                tmp = (values[i*y_size+PI] - values[(i-1)*y_size+PCI]);
                if(tmp == 0)tmp = 0.00001;
                values[i*y_size+PBI] = -1 * values[i*y_size+BI] * values[i*y_size+BI] / tmp;
                values[i*y_size+UBI] = (values[i*y_size+PI] * values[i*y_size+UI] - values[(i-1)*y_size+PCI] * values[(i-1)*y_size+UCI]) / values[i*y_size+BI];
            }

            if(values[i*y_size+CI] !=0){
                tmp = (values[i*y_size+PI] - values[(i+1)*y_size+PBI]);
                if(tmp == 0) tmp = 0.00001;
                values[i*y_size+PCI] = -1 * values[i*y_size+CI] * values[i*y_size+CI] / tmp;
                values[i*y_size+UCI] = (values[i*y_size+PI] * values[i*y_size+UI] - values[(i+1)*y_size+PBI] * values[(i+1)*y_size+UBI]) / values[i*y_size+CI];

            }
        

        }

        for(i=1;i<values.size()/y_size-1;i++){
            tmp_ui_after[i] = values[i*y_size+B];
            tmp_pi_after[i] = values[i*y_size+AI];

            if(values[i*y_size+CI] != 0){
                tmp_ui_after[i] = tmp_ui_after[i] + values[(i+1)*y_size+PBI] * values[(i+1)*y_size+UBI];
                tmp_pi_after[i] = tmp_pi_after[i] + values[(i+1)*y_size+PBI];
            }

            if(values[i*y_size] != 0){
                tmp_ui_after[i] = tmp_ui_after[i] + values[(i-1)*y_size+PCI] * values[(i-1)*y_size+UCI];
                tmp_pi_after[i] = tmp_pi_after[i] + values[(i-1)*y_size+PCI];
            }

            double x_before = tmp_ui_before[i] / tmp_pi_before[i];
            double x_after = tmp_ui_after[i] / tmp_pi_after[i];
            if(x_after > x_before){
                err += x_after - x_before;
            }else{
                err += x_before - x_after;
            }

            for(j=0;j<y_size-1;j++){
                results.push_back(values[i*y_size+j]);
            }
            if(i==1 || i==values.size()/y_size-2){
                results.push_back(err);
            }else{
                results.push_back(0);
            }
        }

        if(values[values.size()-y_size+BI] == 0 && values[values.size()-y_size+AI] == 0 && values[values.size()-y_size+CI] == 0){
            for(j=0;j<y_size;j++){
                results.push_back(values[values.size()-y_size + j]);
            }

        }
        end_time = clock();
//        std::cout << "iter " << iter << " run time is " << (end_time - start_time)/1000000 << "ms" << std::endl;
                return results;

        },y_size,1,1);
        if(++iter > 20000) break;
    }

    nums = nums.InterMap2D([y_size](std::vector<double> values) {
        std::vector<double> results;
        int i;

        for(i=1;i<values.size()/y_size-1;i++){
            values[i*y_size+PI] = values[i*y_size+AI];
            values[i*y_size+UI] = values[i*y_size+B];

            if(values[i*y_size+CI] != 0){
                values[i*y_size+PI] = values[i*y_size+PI] + values[(i+1)*y_size+PBI];
                values[i*y_size+UI] = values[i*y_size+UI] + values[(i+1)*y_size+PBI] * values[(i+1)*y_size+UBI];
            }

            if(values[i*y_size] != 0){
                values[i*y_size+PI] = values[i*y_size+PI] + values[(i-1)*y_size+PCI];
                values[i*y_size+UI] = values[i*y_size+UI] + values[(i-1)*y_size+PCI] * values[(i-1)*y_size+UCI];
            }
            values[i*y_size+X] = values[i*y_size+UI] / values[i*y_size+PI];
            results.push_back(values[i*y_size+X]);
        }
 
        //for(i=1;i<values.size()/y_size-1;i++){
        //   std::cout << "x[" << i << "]=" << values[i*y_size+X] << " ";
        //}  
        return results;
        },y_size,1,1);
        

    nums.Map([](const double num){

        return std::to_string(num);
    })
    .WriteLines(output);


}

/******************************************************************************/

int main(int argc, char* argv[]) {

    tlx::CmdlineParser clp;

    std::string output;
    clp.add_string('o', "output", output,
                   "output file pattern");
    std::vector<std::string> input;
    clp.add_param_stringlist("input", input,
                             "input file pattern(s)");

    if (!clp.process(argc, argv)) {
        return -1;
    }

    clp.print_result();

    return api::Run(
        [&](api::Context& ctx) {

           RunGaBP(ctx, 13, input,output);
         
        });
}

/******************************************************************************/
