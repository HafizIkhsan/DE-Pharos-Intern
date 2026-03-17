CREATE TABLE outlets (
    outlet_code VARCHAR PRIMARY KEY,
    outlet_name VARCHAR
);

CREATE TABLE products (
    product_code VARCHAR PRIMARY KEY,
    product_name VARCHAR
);

CREATE TABLE outlets_products (
    outlet_code VARCHAR,
    product_code VARCHAR,
    product_price NUMERIC,
    PRIMARY KEY (outlet_code, product_code),
    FOREIGN KEY (outlet_code) REFERENCES outlets(outlet_code),
    FOREIGN KEY (product_code) REFERENCES products(product_code)
);

CREATE TABLE sales (
    sales_period DATE,
    outlet_code VARCHAR,
    product_code VARCHAR,
    qty INTEGER,
    actual_sales NUMERIC,
    PRIMARY KEY (sales_period, outlet_code, product_code),
    FOREIGN KEY (outlet_code) REFERENCES outlets(outlet_code),
    FOREIGN KEY (product_code) REFERENCES products(product_code)
);