-- #loadConfig
insert into bmsql_config (cfg_name, cfg_value)
values (?, ?);

-- #loadItem
insert into bmsql_item (i_id, i_im_id, i_name, i_price, i_data)
values (?, ?, ?, ?, ?);

-- #loadWarehouse
insert into bmsql_warehouse (
  w_id, w_name, w_street_1, w_street_2, w_city,
  w_state, w_zip, w_tax, w_ytd)
values (?, ?, ?, ?, ?, ?, ?, ?, ?);

-- #loadStock
insert into bmsql_stock (s_i_id, s_w_id, s_quantity, s_dist_01, s_dist_02,
  s_dist_03, s_dist_04, s_dist_05, s_dist_06,
  s_dist_07, s_dist_08, s_dist_09, s_dist_10,
  s_ytd, s_order_cnt, s_remote_cnt, s_data)
values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- #loadDistrict
insert into bmsql_district (
  d_id, d_w_id, d_name, d_street_1, d_street_2,
  d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id)
values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- #loadCustomer
insert into bmsql_customer (
  c_id, c_d_id, c_w_id, c_first, c_middle, c_last,
  c_street_1, c_street_2, c_city, c_state, c_zip,
  c_phone, c_since, c_credit, c_credit_lim, c_discount,
  c_balance, c_ytd_payment, c_payment_cnt,
  c_delivery_cnt, c_data)
values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- #loadHistory
insert into bmsql_history (
  hist_id, h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id,
  h_date, h_amount, h_data)
values (?, ?, ?, ?, ?, ?, ?, ?, ?);

-- #loadOrder
insert into bmsql_oorder (
  o_id, o_d_id, o_w_id, o_c_id, o_entry_d,
  o_carrier_id, o_ol_cnt, o_all_local)
values (?, ?, ?, ?, ?, ?, ?, ?);

-- #loadOrderLine
insert into bmsql_order_line (
  ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id,
  ol_supply_w_id, ol_delivery_d, ol_quantity,
  ol_amount, ol_dist_info)
values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- #loadNewOrder
insert into bmsql_new_order (no_o_id, no_d_id, no_w_id)
values (?, ?, ?);