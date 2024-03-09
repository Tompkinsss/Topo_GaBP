/*******************************************************************************
 * thrill/common/matrix.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_NDARRAY_HEADER
#define THRILL_COMMON_NDARRAY_HEADER

#include <vector>

namespace thrill {
namespace common {


    template <typename T> T getElement(std::vector<T> data, size_t dim_y, size_t x, size_t y){
        return data[x * dim_y + y];
    }
    template <typename T> T getElement(std::vector<T> data, size_t dim_x, size_t dim_y, size_t x, size_t y, size_t z){
        return data[z * dim_x * dim_y + x * dim_y +y];
    }
} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_NDARRAY_HEADER

/******************************************************************************/
