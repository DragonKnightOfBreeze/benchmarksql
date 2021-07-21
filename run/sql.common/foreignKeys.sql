alter table bmsql_district add constraint d_warehouse_fkey
    foreign key (d_w_id)
    references bmsql_warehouse (w_id);

alter table bmsql_customer add constraint c_district_fkey
    foreign key (c_w_id, c_d_id)
    references bmsql_district (d_w_id, d_id);

alter table bmsql_history add constraint h_district_fkey
    foreign key (h_w_id, h_d_id)
    references bmsql_district (d_w_id, d_id);

alter table bmsql_new_order add constraint no_order_fkey
    foreign key (no_w_id, no_d_id, no_o_id)
    references bmsql_oorder (o_w_id, o_d_id, o_id);

alter table bmsql_oorder add constraint o_customer_fkey
    foreign key (o_w_id, o_d_id, o_c_id)
    references bmsql_customer (c_w_id, c_d_id, c_id);

alter table bmsql_order_line add constraint ol_order_fkey
    foreign key (ol_w_id, ol_d_id, ol_o_id)
    references bmsql_oorder (o_w_id, o_d_id, o_id);

alter table bmsql_stock add constraint s_warehouse_fkey
    foreign key (s_w_id)
    references bmsql_warehouse (w_id);
