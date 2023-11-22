from airflow import DAG
from datetime import datetime, timedelta
from cloudera.cdp.airflow.operators.cdw_operator import CDWOperator
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable

LOCATION = Variable.get("LOCATION")

default_args = {
    'owner': 'frothkoetter',
    'depends_on_past': False,
    'email': ['frothkoetter@cloudera.com'],
    'start_date': datetime(2021,1,1,1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'tpcds-dag', default_args=default_args, catchup=False, is_paused_upon_creation=False)

tpcds_cleanup = """
CREATE DATABASE IF NOT EXISTS tpcds_ws;
CREATE DATABASE IF NOT EXISTS tpcds_ice_ws;
"""

tpcds_ext = """
use tpcds_ws;
set hivevar:LOCATION=s3a://goes-se-sandbox01/tpcds/1000;

-- Table<store_sales (23 cols)  partition=ss_sold_date_sk>

drop table if exists store_sales;
create external table if not exists store_sales(
      ss_sold_date_sk bigint

,     ss_sold_time_sk bigint
,     ss_item_sk bigint
,     ss_customer_sk bigint
,     ss_cdemo_sk bigint
,     ss_hdemo_sk bigint
,     ss_addr_sk bigint
,     ss_store_sk bigint
,     ss_promo_sk bigint
,     ss_ticket_number bigint
,     ss_quantity int
,     ss_wholesale_cost decimal(7,2)
,     ss_list_price decimal(7,2)
,     ss_sales_price decimal(7,2)
,     ss_ext_discount_amt decimal(7,2)
,     ss_ext_sales_price decimal(7,2)
,     ss_ext_wholesale_cost decimal(7,2)
,     ss_ext_list_price decimal(7,2)
,     ss_ext_tax decimal(7,2)
,     ss_coupon_amt decimal(7,2)
,     ss_net_paid decimal(7,2)
,     ss_net_paid_inc_tax decimal(7,2)
,     ss_net_profit decimal(7,2)  
)
row format delimited fields terminated by '|'
location '${LOCATION}/store_sales'
;

-- Table<store_returns (20 cols)  partition=sr_returned_date_sk>

drop table if exists store_returns;
create external table if not exists store_returns(
      sr_returned_date_sk bigint
,     sr_return_time_sk bigint
,     sr_item_sk bigint
,     sr_customer_sk bigint
,     sr_cdemo_sk bigint
,     sr_hdemo_sk bigint
,     sr_addr_sk bigint
,     sr_store_sk bigint
,     sr_reason_sk bigint
,     sr_ticket_number bigint
,     sr_return_quantity int
,     sr_return_amt decimal(7,2)
,     sr_return_tax decimal(7,2)
,     sr_return_amt_inc_tax decimal(7,2)
,     sr_fee decimal(7,2)
,     sr_return_ship_cost decimal(7,2)
,     sr_refunded_cash decimal(7,2)
,     sr_reversed_charge decimal(7,2)
,     sr_store_credit decimal(7,2)
,     sr_net_loss decimal(7,2)
)
row format delimited fields terminated by '|' 
location '${LOCATION}/store_returns'
;

-- Table<catalog_sales (34 cols)  partition=cs_sold_date_sk>

drop table if exists catalog_sales;
create external table if not exists catalog_sales(
      cs_sold_date_sk bigint
,     cs_sold_time_sk bigint
,     cs_ship_date_sk bigint
,     cs_bill_customer_sk bigint
,     cs_bill_cdemo_sk bigint
,     cs_bill_hdemo_sk bigint
,     cs_bill_addr_sk bigint
,     cs_ship_customer_sk bigint
,     cs_ship_cdemo_sk bigint
,     cs_ship_hdemo_sk bigint
,     cs_ship_addr_sk bigint
,     cs_call_center_sk bigint
,     cs_catalog_page_sk bigint
,     cs_ship_mode_sk bigint
,     cs_warehouse_sk bigint
,     cs_item_sk bigint
,     cs_promo_sk bigint
,     cs_order_number bigint
,     cs_quantity int
,     cs_wholesale_cost decimal(7,2)
,     cs_list_price decimal(7,2)
,     cs_sales_price decimal(7,2)
,     cs_ext_discount_amt decimal(7,2)
,     cs_ext_sales_price decimal(7,2)
,     cs_ext_wholesale_cost decimal(7,2)
,     cs_ext_list_price decimal(7,2)
,     cs_ext_tax decimal(7,2)
,     cs_coupon_amt decimal(7,2)
,     cs_ext_ship_cost decimal(7,2)
,     cs_net_paid decimal(7,2)
,     cs_net_paid_inc_tax decimal(7,2)
,     cs_net_paid_inc_ship decimal(7,2)
,     cs_net_paid_inc_ship_tax decimal(7,2)
,     cs_net_profit decimal(7,2)
)
row format delimited fields terminated by '|' 
location '${LOCATION}/catalog_sales'
;

-- Table<catalog_returns (27 cols)  partition=cr_returned_date_sk>

drop table if exists catalog_returns;
create external table if not exists catalog_returns(
      cr_returned_date_sk bigint
,     cr_returned_time_sk bigint
,     cr_item_sk bigint
,     cr_refunded_customer_sk bigint
,     cr_refunded_cdemo_sk bigint
,     cr_refunded_hdemo_sk bigint
,     cr_refunded_addr_sk bigint
,     cr_returning_customer_sk bigint
,     cr_returning_cdemo_sk bigint
,     cr_returning_hdemo_sk bigint
,     cr_returning_addr_sk bigint
,     cr_call_center_sk bigint
,     cr_catalog_page_sk bigint
,     cr_ship_mode_sk bigint
,     cr_warehouse_sk bigint
,     cr_reason_sk bigint
,     cr_order_number bigint
,     cr_return_quantity int
,     cr_return_amount decimal(7,2)
,     cr_return_tax decimal(7,2)
,     cr_return_amt_inc_tax decimal(7,2)
,     cr_fee decimal(7,2)
,     cr_return_ship_cost decimal(7,2)
,     cr_refunded_cash decimal(7,2)
,     cr_reversed_charge decimal(7,2)
,     cr_store_credit decimal(7,2)
,     cr_net_loss decimal(7,2)  
)
row format delimited fields terminated by '|' 
location '${LOCATION}/catalog_returns'
;

-- Table<web_sales (34 cols)  partition=ws_sold_date_sk>

drop table if exists web_sales;
create external table if not exists web_sales(
      ws_sold_date_sk bigint
,     ws_sold_time_sk bigint
,     ws_ship_date_sk bigint
,     ws_item_sk bigint
,     ws_bill_customer_sk bigint
,     ws_bill_cdemo_sk bigint
,     ws_bill_hdemo_sk bigint
,     ws_bill_addr_sk bigint
,     ws_ship_customer_sk bigint
,     ws_ship_cdemo_sk bigint
,     ws_ship_hdemo_sk bigint
,     ws_ship_addr_sk bigint
,     ws_web_page_sk bigint
,     ws_web_site_sk bigint
,     ws_ship_mode_sk bigint
,     ws_warehouse_sk bigint
,     ws_promo_sk bigint
,     ws_order_number bigint
,     ws_quantity int
,     ws_wholesale_cost decimal(7,2)
,     ws_list_price decimal(7,2)
,     ws_sales_price decimal(7,2)
,     ws_ext_discount_amt decimal(7,2)
,     ws_ext_sales_price decimal(7,2)
,     ws_ext_wholesale_cost decimal(7,2)
,     ws_ext_list_price decimal(7,2)
,     ws_ext_tax decimal(7,2)
,     ws_coupon_amt decimal(7,2)
,     ws_ext_ship_cost decimal(7,2)
,     ws_net_paid decimal(7,2)
,     ws_net_paid_inc_tax decimal(7,2)
,     ws_net_paid_inc_ship decimal(7,2)
,     ws_net_paid_inc_ship_tax decimal(7,2)
,     ws_net_profit decimal(7,2)
)
row format delimited fields terminated by '|' 
location '${LOCATION}/web_sales'
;

-- Table<web_returns (24 cols)  partition=wr_returned_date_sk>

drop table if exists web_returns;
create external table if not exists web_returns(
      wr_returned_date_sk bigint
,     wr_returned_time_sk bigint
,     wr_item_sk bigint
,     wr_refunded_customer_sk bigint
,     wr_refunded_cdemo_sk bigint
,     wr_refunded_hdemo_sk bigint
,     wr_refunded_addr_sk bigint
,     wr_returning_customer_sk bigint
,     wr_returning_cdemo_sk bigint
,     wr_returning_hdemo_sk bigint
,     wr_returning_addr_sk bigint
,     wr_web_page_sk bigint
,     wr_reason_sk bigint
,     wr_order_number bigint
,     wr_return_quantity int
,     wr_return_amt decimal(7,2)
,     wr_return_tax decimal(7,2)
,     wr_return_amt_inc_tax decimal(7,2)
,     wr_fee decimal(7,2)
,     wr_return_ship_cost decimal(7,2)
,     wr_refunded_cash decimal(7,2)
,     wr_reversed_charge decimal(7,2)
,     wr_account_credit decimal(7,2)
,     wr_net_loss decimal(7,2) 
)
row format delimited fields terminated by '|' 
location '${LOCATION}/web_returns'
;

-- Table<inventory (4 cols)>

drop table if exists inventory;
create external table if not exists inventory(
      inv_date_sk bigint
,     inv_item_sk bigint
,     inv_warehouse_sk bigint
,     inv_quantity_on_hand int
)
row format delimited fields terminated by '|'
location '${LOCATION}/inventory';

-- Table<store (29 cols)>

drop table if exists store;
create external table if not exists store(
      s_store_sk bigint
,     s_store_id char(16)
,     s_rec_start_date date
,     s_rec_end_date date
,     s_closed_date_sk bigint
,     s_store_name varchar(50)
,     s_number_employees int
,     s_floor_space int
,     s_hours char(20)
,     S_manager varchar(40)
,     S_market_id int
,     S_geography_class varchar(100)
,     S_market_desc varchar(100)
,     s_market_manager varchar(40)
,     s_division_id int
,     s_division_name varchar(50)
,     s_company_id int
,     s_company_name varchar(50)
,     s_street_number varchar(10)
,     s_street_name varchar(60)
,     s_street_type char(15)
,     s_suite_number char(10)
,     s_city varchar(60)
,     s_county varchar(30)
,     s_state char(2)
,     s_zip char(10)
,     s_country varchar(20)
,     s_gmt_offset decimal(5,2)
,     s_tax_percentage decimal(5,2)
)
row format delimited fields terminated by '|'
location '${LOCATION}/store';

-- Table<call_center (31 cols)>

drop table if exists call_center;
create external table if not exists call_center(
      cc_call_center_sk bigint
,     cc_call_center_id char(16)
,     cc_rec_start_date date
,     cc_rec_end_date date
,     cc_closed_date_sk bigint
,     cc_open_date_sk bigint
,     cc_name varchar(50)
,     cc_class varchar(50)
,     cc_employees int
,     cc_sq_ft int
,     cc_hours char(20)
,     cc_manager varchar(40)
,     cc_mkt_id int
,     cc_mkt_class char(50)
,     cc_mkt_desc varchar(100)
,     cc_market_manager varchar(40)
,     cc_division int
,     cc_division_name varchar(50)
,     cc_company int
,     cc_company_name char(50)
,     cc_street_number char(10)
,     cc_street_name varchar(60)
,     cc_street_type char(15)
,     cc_suite_number char(10)
,     cc_city varchar(60)
,     cc_county varchar(30)
,     cc_state char(2)
,     cc_zip char(10)
,     cc_country varchar(20)
,     cc_gmt_offset decimal(5,2)
,     cc_tax_percentage decimal(5,2)
)
row format delimited fields terminated by '|'
location '${LOCATION}/call_center';

-- Table<catalog_page (9 cols)>

drop table if exists catalog_page;
create external table if not exists catalog_page(
      cp_catalog_page_sk bigint
,     cp_catalog_page_id char(16)
,     cp_start_date_sk bigint
,     cp_end_date_sk bigint
,     cp_department varchar(50)
,     cp_catalog_number int
,     cp_catalog_page_number int
,     cp_description varchar(100)
,     cp_type varchar(100)
)
row format delimited fields terminated by '|'
location '${LOCATION}/catalog_page';

-- Table<web_site (26 cols)>

drop table if exists web_site;
create external table if not exists web_site(
      web_site_sk bigint
,     web_site_id char(16)
,     web_rec_start_date date
,     web_rec_end_date date
,     web_name varchar(50)
,     web_open_date_sk bigint
,     web_close_date_sk bigint
,     web_class varchar(50)
,     web_manager varchar(40)
,     web_mkt_id int
,     web_mkt_class varchar(50)
,     web_mkt_desc varchar(100)
,     web_market_manager varchar(40)
,     web_company_id int
,     web_company_name char(50)
,     web_street_number char(10)
,     web_street_name varchar(60)
,     web_street_type char(15)
,     web_suite_number char(10)
,     web_city varchar(60)
,     web_county varchar(30)
,     web_state char(2)
,     web_zip char(10)
,     web_country varchar(20)
,     web_gmt_offset decimal(5,2)  
,     web_tax_percentage decimal(5,2)
)
row format delimited fields terminated by '|'
location '${LOCATION}/web_site';

-- Table<web_page (14 cols)>

drop table if exists web_page;
create external table if not exists web_page(
      wp_web_page_sk bigint
,     wp_web_page_id char(16)
,     wp_rec_start_date date
,     wp_rec_end_date date
,     wp_creation_date_sk bigint
,     wp_access_date_sk bigint
,     wp_autogen_flag char(1)
,     wp_customer_sk bigint
,     wp_url varchar(100)
,     wp_type char(50)
,     wp_char_count int
,     wp_link_count int
,     wp_image_count int
,     wp_max_ad_count int
)
row format delimited fields terminated by '|'
location '${LOCATION}/web_page';

-- Table<warehouse (14 cols)>

drop table if exists warehouse;
create external table if not exists warehouse(
      w_warehouse_sk bigint
,     w_warehouse_id char(16)
,     w_warehouse_name varchar(20)
,     w_warehouse_sq_ft int
,     w_street_number char(10)
,     w_street_name varchar(60)
,     w_street_type char(15)
,     w_suite_number char(10)
,     w_city varchar(60)
,     w_county varchar(30)
,     w_state char(2)
,     w_zip char(10)
,     w_country varchar(20)
,     w_gmt_offset decimal(5,2)
)
row format delimited fields terminated by '|'
location '${LOCATION}/warehouse';

-- Table<customer (18 cols)>

drop table if exists customer;
create external table if not exists customer(
      c_customer_sk bigint
,     c_customer_id char(16)
,     c_current_cdemo_sk bigint
,     c_current_hdemo_sk bigint
,     c_current_addr_sk bigint
,     c_first_shipto_date_sk bigint
,     c_first_sales_date_sk bigint
,     c_salutation char(10)
,     c_first_name char(20)
,     c_last_name char(30)
,     c_preferred_cust_flag char(1)
,     c_birth_day int
,     c_birth_month int
,     c_birth_year int
,     c_birth_country varchar(20)
,     c_login char(13)
,     c_email_address char(50)
,     c_last_review_date_sk bigint
)
row format delimited fields terminated by '|'
location '${LOCATION}/customer';

-- Table<customer_address (13 cols)>

drop table if exists customer_address;
create external table if not exists customer_address(
      ca_address_sk bigint
,     ca_address_id char(16)
,     ca_street_number char(10)
,     ca_street_name varchar(60)
,     ca_street_type char(15)
,     ca_suite_number char(10)
,     ca_city varchar(60)
,     ca_county varchar(30)
,     ca_state char(2)
,     ca_zip char(10)
,     ca_country varchar(20)
,     ca_gmt_offset decimal(5,2)
,     ca_location_type char(20)
)
row format delimited fields terminated by '|'
location '${LOCATION}/customer_address';

-- Table<customer_demographics (9 cols)>

drop table if exists customer_demographics;
create external table if not exists customer_demographics(
      cd_demo_sk bigint
,     cd_gender char(1)
,     cd_marital_status char(1)
,     cd_education_status char(20)
,     cd_purchase_estimate int
,     cd_credit_rating char(10)
,     cd_dep_count int
,     cd_dep_employed_count int
,     cd_dep_college_count int
)
row format delimited fields terminated by '|'
location '${LOCATION}/customer_demographics';

-- Table<date_dim (28 cols)>

drop table if exists date_dim;
create external table if not exists date_dim(
      d_date_sk bigint
,     d_date_id char(16)
,     d_date date
,     d_month_seq int
,     d_week_seq int
,     d_quarter_seq int
,     d_year int
,     d_dow int
,     d_moy int
,     d_dom int
,     d_qoy int
,     d_fy_year int
,     d_fy_quarter_seq int
,     d_fy_week_seq int
,     d_day_name char(9)
,     d_quarter_name char(6)
,     d_holiday char(1)
,     d_weekend char(1)
,     d_following_holiday char(1)
,     d_first_dom int
,     d_last_dom int
,     d_same_day_ly int
,     d_same_day_lq int
,     d_current_day char(1)
,     d_current_week char(1)
,     d_current_month char(1)
,     d_current_quarter char(1)
,     d_current_year char(1)
)
row format delimited fields terminated by '|'
location '${LOCATION}/date_dim';

-- Table<household_demographics (5 cols)>

drop table if exists household_demographics;
create external table if not exists household_demographics(
      hd_demo_sk bigint
,     hd_income_band_sk bigint
,     hd_buy_potential char(15)
,     hd_dep_count int
,     hd_vehicle_count int
)
row format delimited fields terminated by '|'
location '${LOCATION}/household_demographics';

-- Table<item (22 cols)>

drop table if exists item;
create external table if not exists item(
      i_item_sk bigint
,     i_item_id char(16)
,     i_rec_start_date date
,     i_rec_end_date date
,     i_item_desc varchar(200)
,     i_current_price decimal(7,2)
,     i_wholesale_cost decimal(7,2)
,     i_brand_id int
,     i_brand char(50)
,     i_class_id int
,     i_class char(50)
,     i_category_id int
,     i_category char(50)
,     i_manufact_id int
,     i_manufact char(50)
,     i_size char(20)
,     i_formulation char(20)
,     i_color char(20)
,     i_units char(10)
,     i_container char(10)
,     i_manager_id int
,     i_product_name char(50)
)
row format delimited fields terminated by '|'
location '${LOCATION}/item';

-- Table<income_band (3 cols)>

drop table if exists income_band;
create external table if not exists income_band(
      ib_income_band_sk bigint
,     ib_lower_bound int
,     ib_upper_bound int
)
row format delimited fields terminated by '|'
location '${LOCATION}/income_band';

-- Table<promotion (19 cols)>

drop table if exists promotion;
create external table if not exists promotion(
      p_promo_sk bigint
,     p_promo_id char(16)
,     p_start_date_sk bigint
,     p_end_date_sk bigint
,     p_item_sk bigint
,     p_cost decimal(15,2)
,     p_response_target int
,     p_promo_name char(50)
,     p_channel_dmail char(1)
,     p_channel_email char(1)
,     p_channel_catalog char(1)
,     p_channel_tv char(1)
,     p_channel_radio char(1)
,     p_channel_press char(1)
,     p_channel_event char(1)
,     p_channel_demo char(1)
,     p_channel_details varchar(100)
,     p_purpose char(15)
,     p_discount_active char(1)
)
row format delimited fields terminated by '|'
location '${LOCATION}/promotion';

-- Table<reason (3 cols)>

drop table if exists reason;
create external table if not exists reason(
      r_reason_sk bigint
,     r_reason_id char(16)
,     r_reason_desc char(100)
)
row format delimited fields terminated by '|'
location '${LOCATION}/reason';

-- Table<ship_mode (6 cols)>

drop table if exists ship_mode;
create external table if not exists ship_mode(
      sm_ship_mode_sk bigint
,     sm_ship_mode_id char(16)
,     sm_type char(30)
,     sm_code char(10)
,     sm_carrier char(20)
,     sm_contract char(20)
)
row format delimited fields terminated by '|'
location '${LOCATION}/ship_mode';

-- Table<time_dim (10 cols)>

drop table if exists time_dim;
create external table if not exists time_dim(
      t_time_sk bigint
,     t_time_id char(16)
,     t_time int
,     t_hour int
,     t_minute int
,     t_second int
,     t_am_pm char(2)
,     t_shift char(20)
,     t_sub_shift char(20)
,     t_meal_time char(20)
)
row format delimited fields terminated by '|'
location '${LOCATION}/time_dim';
"""

tpcds_load = """
use tpcds_ice_ws;
set hivevar:FILE=ICEBERG;
set hivevar:SOURCE=tpcds_ws;
set iceberg.mr.schema.auto.conversion=true;

drop table if exists call_center;

create table call_center
stored by ${FILE}
as select * from ${SOURCE}.call_center;

drop table if exists catalog_page;

create table catalog_page
stored by ${FILE}
as select * from ${SOURCE}.catalog_page;

drop table if exists catalog_returns;

create table catalog_returns
(
      cr_returned_time_sk bigint
,     cr_item_sk bigint
,     cr_refunded_customer_sk bigint
,     cr_refunded_cdemo_sk bigint
,     cr_refunded_hdemo_sk bigint
,     cr_refunded_addr_sk bigint
,     cr_returning_customer_sk bigint
,     cr_returning_cdemo_sk bigint
,     cr_returning_hdemo_sk bigint
,     cr_returning_addr_sk bigint
,     cr_call_center_sk bigint
,     cr_catalog_page_sk bigint
,     cr_ship_mode_sk bigint
,     cr_warehouse_sk bigint
,     cr_reason_sk bigint
,     cr_order_number bigint
,     cr_return_quantity int
,     cr_return_amount decimal(7,2)
,     cr_return_tax decimal(7,2)
,     cr_return_amt_inc_tax decimal(7,2)
,     cr_fee decimal(7,2)
,     cr_return_ship_cost decimal(7,2)
,     cr_refunded_cash decimal(7,2)
,     cr_reversed_charge decimal(7,2)
,     cr_store_credit decimal(7,2)
,     cr_net_loss decimal(7,2)
)
partitioned by (cr_returned_date_sk bigint)
stored by ${FILE};

from ${SOURCE}.catalog_returns cr
insert overwrite table catalog_returns 
select
        cr.cr_returned_time_sk,
        cr.cr_item_sk,
        cr.cr_refunded_customer_sk,
        cr.cr_refunded_cdemo_sk,
        cr.cr_refunded_hdemo_sk,
        cr.cr_refunded_addr_sk,
        cr.cr_returning_customer_sk,
        cr.cr_returning_cdemo_sk,
        cr.cr_returning_hdemo_sk,
        cr.cr_returning_addr_sk,
        cr.cr_call_center_sk,
        cr.cr_catalog_page_sk,
        cr.cr_ship_mode_sk,
        cr.cr_warehouse_sk,
        cr.cr_reason_sk,
        cr.cr_order_number,
        cr.cr_return_quantity,
        cr.cr_return_amount,
        cr.cr_return_tax,
        cr.cr_return_amt_inc_tax,
        cr.cr_fee,
        cr.cr_return_ship_cost,
        cr.cr_refunded_cash,
        cr.cr_reversed_charge,
        cr.cr_store_credit,
        cr.cr_net_loss,
        cr.cr_returned_date_sk
      where cr.cr_returned_date_sk is not null
;

drop table if exists catalog_sales;

create table catalog_sales
(
      cs_sold_time_sk bigint
,     cs_ship_date_sk bigint
,     cs_bill_customer_sk bigint
,     cs_bill_cdemo_sk bigint
,     cs_bill_hdemo_sk bigint
,     cs_bill_addr_sk bigint
,     cs_ship_customer_sk bigint
,     cs_ship_cdemo_sk bigint
,     cs_ship_hdemo_sk bigint
,     cs_ship_addr_sk bigint
,     cs_call_center_sk bigint
,     cs_catalog_page_sk bigint
,     cs_ship_mode_sk bigint
,     cs_warehouse_sk bigint
,     cs_item_sk bigint
,     cs_promo_sk bigint
,     cs_order_number bigint
,     cs_quantity int
,     cs_wholesale_cost decimal(7,2)
,     cs_list_price decimal(7,2)
,     cs_sales_price decimal(7,2)
,     cs_ext_discount_amt decimal(7,2)
,     cs_ext_sales_price decimal(7,2)
,     cs_ext_wholesale_cost decimal(7,2)
,     cs_ext_list_price decimal(7,2)
,     cs_ext_tax decimal(7,2)
,     cs_coupon_amt decimal(7,2)
,     cs_ext_ship_cost decimal(7,2)
,     cs_net_paid decimal(7,2)
,     cs_net_paid_inc_tax decimal(7,2)
,     cs_net_paid_inc_ship decimal(7,2)
,     cs_net_paid_inc_ship_tax decimal(7,2)
,     cs_net_profit decimal(7,2)
)
partitioned by (cs_sold_date_sk bigint)
stored by ${FILE};

from ${SOURCE}.catalog_sales cs
insert overwrite table catalog_sales 
select
        cs.cs_sold_time_sk,
        cs.cs_ship_date_sk,
        cs.cs_bill_customer_sk,
        cs.cs_bill_cdemo_sk,
        cs.cs_bill_hdemo_sk,
        cs.cs_bill_addr_sk,
        cs.cs_ship_customer_sk,
        cs.cs_ship_cdemo_sk,
        cs.cs_ship_hdemo_sk,
        cs.cs_ship_addr_sk,
        cs.cs_call_center_sk,
        cs.cs_catalog_page_sk,
        cs.cs_ship_mode_sk,
        cs.cs_warehouse_sk,
        cs.cs_item_sk,
        cs.cs_promo_sk,
        cs.cs_order_number,
        cs.cs_quantity,
        cs.cs_wholesale_cost,
        cs.cs_list_price,
        cs.cs_sales_price,
        cs.cs_ext_discount_amt,
        cs.cs_ext_sales_price,
        cs.cs_ext_wholesale_cost,
        cs.cs_ext_list_price,
        cs.cs_ext_tax,
        cs.cs_coupon_amt,
        cs.cs_ext_ship_cost,
        cs.cs_net_paid,
        cs.cs_net_paid_inc_tax,
        cs.cs_net_paid_inc_ship,
        cs.cs_net_paid_inc_ship_tax,
        cs.cs_net_profit,
        cs.cs_sold_date_sk
        where cs.cs_sold_date_sk is not null
;

drop table if exists customer;

create table customer
stored by ${FILE}
as select * from ${SOURCE}.customer
CLUSTER BY c_customer_sk
;

drop table if exists customer_address;

create table customer_address
stored by ${FILE}
as select * from ${SOURCE}.customer_address 
CLUSTER BY ca_address_sk
;

drop table if exists customer_demographics;

create table customer_demographics
stored by ${FILE}
as select * from ${SOURCE}.customer_demographics;

drop table if exists date_dim;

create table date_dim
stored by ${FILE}
as select * from ${SOURCE}.date_dim;

drop table if exists household_demographics;

create table household_demographics
stored by ${FILE}
as select * from ${SOURCE}.household_demographics;

drop table if exists income_band;

create table income_band
stored by ${FILE}
as select * from ${SOURCE}.income_band;

drop table if exists inventory;

create table inventory
stored by ${FILE}
as select * from ${SOURCE}.inventory
CLUSTER BY inv_date_sk
;

drop table if exists item;

create table item
stored by ${FILE}
as select * from ${SOURCE}.item
CLUSTER BY i_item_sk
;

drop table if exists promotion;

create table promotion
stored by ${FILE}
as select * from ${SOURCE}.promotion;

drop table if exists reason;

create table reason
stored by ${FILE}
as select * from ${SOURCE}.reason;

drop table if exists ship_mode;

create table ship_mode
stored by ${FILE}
as select * from ${SOURCE}.ship_mode;

drop table if exists store;

create table store
stored by ${FILE}
as select * from ${SOURCE}.store
CLUSTER BY s_store_sk
;

drop table if exists store_returns;

create table store_returns
(
      sr_return_time_sk bigint
,     sr_item_sk bigint
,     sr_customer_sk bigint
,     sr_cdemo_sk bigint
,     sr_hdemo_sk bigint
,     sr_addr_sk bigint
,     sr_store_sk bigint
,     sr_reason_sk bigint
,     sr_ticket_number bigint
,     sr_return_quantity int
,     sr_return_amt decimal(7,2)
,     sr_return_tax decimal(7,2)
,     sr_return_amt_inc_tax decimal(7,2)
,     sr_fee decimal(7,2)
,     sr_return_ship_cost decimal(7,2)
,     sr_refunded_cash decimal(7,2)
,     sr_reversed_charge decimal(7,2)
,     sr_store_credit decimal(7,2)
,     sr_net_loss decimal(7,2)
)
partitioned by (sr_returned_date_sk bigint)
stored by ${FILE};

from ${SOURCE}.store_returns sr
insert overwrite table store_returns
select
        sr.sr_return_time_sk,
        sr.sr_item_sk,
        sr.sr_customer_sk,
        sr.sr_cdemo_sk,
        sr.sr_hdemo_sk,
        sr.sr_addr_sk,
        sr.sr_store_sk,
        sr.sr_reason_sk,
        sr.sr_ticket_number,
        sr.sr_return_quantity,
        sr.sr_return_amt,
        sr.sr_return_tax,
        sr.sr_return_amt_inc_tax,
        sr.sr_fee,
        sr.sr_return_ship_cost,
        sr.sr_refunded_cash,
        sr.sr_reversed_charge,
        sr.sr_store_credit,
        sr.sr_net_loss,
        sr.sr_returned_date_sk
        where sr.sr_returned_date_sk is not null
;

drop table if exists store_sales;

create table store_sales
(
      ss_sold_time_sk bigint
,     ss_item_sk bigint
,     ss_customer_sk bigint
,     ss_cdemo_sk bigint
,     ss_hdemo_sk bigint
,     ss_addr_sk bigint
,     ss_store_sk bigint
,     ss_promo_sk bigint
,     ss_ticket_number bigint
,     ss_quantity int
,     ss_wholesale_cost decimal(7,2)
,     ss_list_price decimal(7,2)
,     ss_sales_price decimal(7,2)
,     ss_ext_discount_amt decimal(7,2)
,     ss_ext_sales_price decimal(7,2)
,     ss_ext_wholesale_cost decimal(7,2)
,     ss_ext_list_price decimal(7,2)
,     ss_ext_tax decimal(7,2)
,     ss_coupon_amt decimal(7,2)
,     ss_net_paid decimal(7,2)
,     ss_net_paid_inc_tax decimal(7,2)
,     ss_net_profit decimal(7,2)
)
partitioned by (ss_sold_date_sk bigint)
stored by ${FILE};

from ${SOURCE}.store_sales ss
insert overwrite table store_sales  
select
        ss.ss_sold_time_sk,
        ss.ss_item_sk,
        ss.ss_customer_sk,
        ss.ss_cdemo_sk,
        ss.ss_hdemo_sk,
        ss.ss_addr_sk,
        ss.ss_store_sk,
        ss.ss_promo_sk,
        ss.ss_ticket_number,
        ss.ss_quantity,
        ss.ss_wholesale_cost,
        ss.ss_list_price,
        ss.ss_sales_price,
        ss.ss_ext_discount_amt,
        ss.ss_ext_sales_price,
        ss.ss_ext_wholesale_cost,
        ss.ss_ext_list_price,
        ss.ss_ext_tax,
        ss.ss_coupon_amt,
        ss.ss_net_paid,
        ss.ss_net_paid_inc_tax,
        ss.ss_net_profit,
        ss.ss_sold_date_sk
        where ss.ss_sold_date_sk is not null
;

drop table if exists time_dim;

create table time_dim
stored by ${FILE}
as select * from ${SOURCE}.time_dim;

drop table if exists warehouse;

create table warehouse
stored by ${FILE}
as select * from ${SOURCE}.warehouse;

drop table if exists web_page;

create table web_page
stored by ${FILE}
as select * from ${SOURCE}.web_page;

drop table if exists web_returns;

create table web_returns
(
      wr_returned_time_sk bigint
,     wr_item_sk bigint
,     wr_refunded_customer_sk bigint
,     wr_refunded_cdemo_sk bigint
,     wr_refunded_hdemo_sk bigint
,     wr_refunded_addr_sk bigint
,     wr_returning_customer_sk bigint
,     wr_returning_cdemo_sk bigint
,     wr_returning_hdemo_sk bigint
,     wr_returning_addr_sk bigint
,     wr_web_page_sk bigint
,     wr_reason_sk bigint
,     wr_order_number bigint
,     wr_return_quantity int
,     wr_return_amt decimal(7,2)
,     wr_return_tax decimal(7,2)
,     wr_return_amt_inc_tax decimal(7,2)
,     wr_fee decimal(7,2)
,     wr_return_ship_cost decimal(7,2)
,     wr_refunded_cash decimal(7,2)
,     wr_reversed_charge decimal(7,2)
,     wr_account_credit decimal(7,2)
,     wr_net_loss decimal(7,2)
)
partitioned by (wr_returned_date_sk       bigint)
stored by ${FILE};

from ${SOURCE}.web_returns wr
insert overwrite table web_returns 
select
        wr.wr_returned_time_sk,
        wr.wr_item_sk,
        wr.wr_refunded_customer_sk,
        wr.wr_refunded_cdemo_sk,
        wr.wr_refunded_hdemo_sk,
        wr.wr_refunded_addr_sk,
        wr.wr_returning_customer_sk,
        wr.wr_returning_cdemo_sk,
        wr.wr_returning_hdemo_sk,
        wr.wr_returning_addr_sk,
        wr.wr_web_page_sk,
        wr.wr_reason_sk,
        wr.wr_order_number,
        wr.wr_return_quantity,
        wr.wr_return_amt,
        wr.wr_return_tax,
        wr.wr_return_amt_inc_tax,
        wr.wr_fee,
        wr.wr_return_ship_cost,
        wr.wr_refunded_cash,
        wr.wr_reversed_charge,
        wr.wr_account_credit,
        wr.wr_net_loss,
		wr.wr_returned_date_sk
        where wr.wr_returned_date_sk is not null
;

drop table if exists web_sales;

create table web_sales
(
    ws_sold_time_sk           bigint,
    ws_ship_date_sk           bigint,
    ws_item_sk                bigint,
    ws_bill_customer_sk       bigint,
    ws_bill_cdemo_sk          bigint,
    ws_bill_hdemo_sk          bigint,
    ws_bill_addr_sk           bigint,
    ws_ship_customer_sk       bigint,
    ws_ship_cdemo_sk          bigint,
    ws_ship_hdemo_sk          bigint,
    ws_ship_addr_sk           bigint,
    ws_web_page_sk            bigint,
    ws_web_site_sk            bigint,
    ws_ship_mode_sk           bigint,
    ws_warehouse_sk           bigint,
    ws_promo_sk               bigint,
    ws_order_number           bigint,
    ws_quantity               int,
    ws_wholesale_cost         decimal(7,2),
    ws_list_price             decimal(7,2),
    ws_sales_price            decimal(7,2),
    ws_ext_discount_amt       decimal(7,2),
    ws_ext_sales_price        decimal(7,2),
    ws_ext_wholesale_cost     decimal(7,2),
    ws_ext_list_price         decimal(7,2),
    ws_ext_tax                decimal(7,2),
    ws_coupon_amt             decimal(7,2),
    ws_ext_ship_cost          decimal(7,2),
    ws_net_paid               decimal(7,2),
    ws_net_paid_inc_tax       decimal(7,2),
    ws_net_paid_inc_ship      decimal(7,2),
    ws_net_paid_inc_ship_tax  decimal(7,2),
    ws_net_profit             decimal(7,2)
)
partitioned by (ws_sold_date_sk           bigint)
stored by ${FILE};

from ${SOURCE}.web_sales ws
insert overwrite table web_sales 
select
        ws.ws_sold_time_sk,
        ws.ws_ship_date_sk,
        ws.ws_item_sk,
        ws.ws_bill_customer_sk,
        ws.ws_bill_cdemo_sk,
        ws.ws_bill_hdemo_sk,
        ws.ws_bill_addr_sk,
        ws.ws_ship_customer_sk,
        ws.ws_ship_cdemo_sk,
        ws.ws_ship_hdemo_sk,
        ws.ws_ship_addr_sk,
        ws.ws_web_page_sk,
        ws.ws_web_site_sk,
        ws.ws_ship_mode_sk,
        ws.ws_warehouse_sk,
        ws.ws_promo_sk,
        ws.ws_order_number,
        ws.ws_quantity,
        ws.ws_wholesale_cost,
        ws.ws_list_price,
        ws.ws_sales_price,
        ws.ws_ext_discount_amt,
        ws.ws_ext_sales_price,
        ws.ws_ext_wholesale_cost,
        ws.ws_ext_list_price,
        ws.ws_ext_tax,
        ws.ws_coupon_amt,
        ws.ws_ext_ship_cost,
        ws.ws_net_paid,
        ws.ws_net_paid_inc_tax,
        ws.ws_net_paid_inc_ship,
        ws.ws_net_paid_inc_ship_tax,
        ws.ws_net_profit,
        ws.ws_sold_date_sk
        where ws.ws_sold_date_sk is not null
;

drop table if exists web_site;

create table web_site
stored by ${FILE}
as select * from ${SOURCE}.web_site;

"""

start = DummyOperator(task_id='start', dag=dag)

cleanup = CDWOperator(
    task_id='cleanup',
    dag=dag,
    cli_conn_id='cdw',
    hql=tpcds_cleanup,
    schema='default',
    use_proxy_user=False,
    query_isolation=True

)

external_tables = CDWOperator(
    task_id='external_tables',
    dag=dag,
    cli_conn_id='cdw',
    hql=tpcds_ext,
    schema='default',
    use_proxy_user=False,
    query_isolation=True
)

load_tables = CDWOperator(
    task_id='load_tables',
    dag=dag,
    cli_conn_id='cdw',
    hql=tpcds_load,
    schema='default',
    use_proxy_user=False,
    query_isolation=True
)

wait = DummyOperator(task_id='wait', dag=dag)

end = DummyOperator(task_id='end', dag=dag)

start >> cleanup >> external_tables  >> load_tables >> end
