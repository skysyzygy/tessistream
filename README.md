
<!-- README.md is generated from README.Rmd. Please edit that file -->

# tessistream

<!-- badges: start -->

[![R-CMD-check](https://github.com/skysyzygy/tessistream/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/skysyzygy/tessistream/actions/workflows/R-CMD-check.yaml)
[![Codecov test
coverage](https://codecov.io/gh/skysyzygy/tessistream/branch/main/graph/badge.svg?token=3R8UJNG6QY)](https://app.codecov.io/gh/skysyzygy/tessistream?branch=main)
<!-- badges: end -->

Builds timestamped datasets based on contributions, ticket sales,
address changes, inventory for historical and predictive analysis.

-   `address_stream` - customer address cleaning plus appends from US
    Census and iWave

-   `benefit_stream` - discounting and other benefits

-   `contribution_stream` - donations and other contributions

-   `email_stream` - email sends, opens, and clicks

-   `inventory_stream` - number of tickets available for sale and hold
    code analysis

-   `membership_stream` - membership starts, ends, and value

-   `ticket_future_stream` - prediction of future ticket purchases based
    on inventory and past buying

-   `ticket_stream` - ticket purchases including discounting information

-   `stream` - union of `address_stream`, `contribution_stream`,
    `email_stream`, `membership_stream`, `ticket_future_stream`, and
    `ticket_stream` for analysis.

## Installation

You can install the development version of tessistream from
[GitHub](https://github.com/) with:

``` r
# install.packages("devtools")
devtools::install_github("skysyzygy/tessistream")

# to install libpostal, run in the RStudio terminal:
scripts/install_libpostal.sh
```

## Example

``` r
library(tessistream)
## basic example code
```
