#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

cd $DIR/input

curl -O "http://s3.amazonaws.com/instacart-datasets/instacart_online_grocery_shopping_2017_05_01.tar.gz"

tar xzvf instacart_online_grocery_shopping_2017_05_01.tar.gz --strip-components 1
mv order_products__prior.csv order_products.csv

/usr/bin/env python3.7 $DIR/src/main.py
