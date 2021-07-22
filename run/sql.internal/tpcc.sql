-- #newOrderSelectWhseCust
select c_discount, c_last, c_credit, w_tax 
    from bmsql_customer 
    join bmsql_warehouse on (w_id = c_w_id) 
    where c_w_id = ? and c_d_id = ? and c_id = ?;
  
-- #newOrderSelectDist
select d_tax, d_next_o_id 
    from bmsql_district 
    where d_w_id = ? and d_id = ? 
    for update;

-- #newOrderUpdateDist
update bmsql_district 
    set d_next_o_id = d_next_o_id + 1 
    where d_w_id = ? and d_id = ?;

-- #mewOrderInsertOrder
insert into bmsql_oorder (
    o_id, o_d_id, o_w_id, o_c_id, o_entry_d, 
    o_ol_cnt, o_all_local) 
values (?, ?, ?, ?, ?, ?, ?);

-- #newOrderInsertNewOrder
insert into bmsql_new_order (
    no_o_id, no_d_id, no_w_id) 
values (?, ?, ?);

-- #newOrderSelectStock
select s_quantity, s_data, 
       s_dist_01, s_dist_02, s_dist_03, s_dist_04, 
       s_dist_05, s_dist_06, s_dist_07, s_dist_08, 
       s_dist_09, s_dist_10 
    from bmsql_stock 
    where s_w_id = ? and s_i_id = ? 
    for update;

-- #newOrderSelectItem
select i_price, i_name, i_data 
    from bmsql_item 
    where i_id = ?;

-- #newOrderUpdateStock
update bmsql_stock 
    set s_quantity = ?, s_ytd = s_ytd + ?, 
        s_order_cnt = s_order_cnt + 1, 
        s_remote_cnt = s_remote_cnt + ? 
    where s_w_id = ? and s_i_id = ?;

-- #newOrderInsertOrderLine
insert into bmsql_order_line (
    ol_o_id, ol_d_id, ol_w_id, ol_number, 
    ol_i_id, ol_supply_w_id, ol_quantity, 
    ol_amount, ol_dist_info) 
values (?, ?, ?, ?, ?, ?, ?, ?, ?);

-- #paymentSelectWarehouse
select w_name, w_street_1, w_street_2, w_city, 
       w_state, w_zip 
    from bmsql_warehouse 
    where w_id = ?;

-- #paymentSelectDistrict
select d_name, d_street_1, d_street_2, d_city, 
       d_state, d_zip 
    from bmsql_district 
    where d_w_id = ? and d_id = ?;

-- #paymentSelectCustomerListByLast
select c_id 
    from bmsql_customer 
    where c_w_id = ? and c_d_id = ? and c_last = ? 
    order by c_first;

-- paymentSelectCustomer
select c_first, c_middle, c_last, c_street_1, c_street_2,
       c_city, c_state, c_zip, c_phone, c_since, c_credit,
       c_credit_lim, c_discount, c_balance
from bmsql_customer
where c_w_id = ? and c_d_id = ? and c_id = ?
  for update;

-- #paymentSelectCustomerData
select c_data 
    from bmsql_customer 
    where c_w_id = ? and c_d_id = ? and c_id = ?;

-- #paymentUpdateWarehouse
update bmsql_warehouse 
    set w_ytd = w_ytd + ? 
    where w_id = ?;

-- #paymentUpdateDistrict
update bmsql_district 
    set d_ytd = d_ytd + ? 
    where d_w_id = ? and d_id = ?;

-- #paymentUpdateCustomer
update bmsql_customer 
    set c_balance = c_balance - ?, 
        c_ytd_payment = c_ytd_payment + ?, 
        c_payment_cnt = c_payment_cnt + 1 
    where c_w_id = ? and c_d_id = ? and c_id = ?;

-- #paymentUpdateCustomerWithData
update bmsql_customer 
    set c_balance = c_balance - ?, 
        c_ytd_payment = c_ytd_payment + ?, 
        c_payment_cnt = c_payment_cnt + 1, 
        c_data = ? 
    where c_w_id = ? and c_d_id = ? and c_id = ?;

-- #paymentInsertHistory
insert into bmsql_history (
    h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, 
    h_date, h_amount, h_data) 
values (?, ?, ?, ?, ?, ?, ?, ?);

-- #orderStatusSelectCustomerListByLast
select c_id 
    from bmsql_customer 
    where c_w_id = ? and c_d_id = ? and c_last = ? 
    order by c_first;

-- #orderStatusSelectCustomer
select c_first, c_middle, c_last, c_balance 
    from bmsql_customer 
    where c_w_id = ? and c_d_id = ? and c_id = ?;

-- #orderStatusSelectLastOrder
select o_id, o_entry_d, o_carrier_id 
    from bmsql_oorder 
    where o_w_id = ? and o_d_id = ? and o_c_id = ? 
      and o_id = (
          select max(o_id) 
              from bmsql_oorder 
              where o_w_id = ? and o_d_id = ? and o_c_id = ?
          );

-- #orderStatusSelectOrderLine
select ol_i_id, ol_supply_w_id, ol_quantity, 
       ol_amount, ol_delivery_d 
    from bmsql_order_line 
    where ol_w_id = ? and ol_d_id = ? and ol_o_id = ? 
    order by ol_w_id, ol_d_id, ol_o_id, ol_number;

-- #stockLevelSelectLow
select count(*) as low_stock from (
    select s_w_id, s_i_id, s_quantity 
        from bmsql_stock 
        where s_w_id = ? and s_quantity < ? and s_i_id in (
            select ol_i_id 
                from bmsql_district 
                join bmsql_order_line on ol_w_id = d_w_id 
                 and ol_d_id = d_id 
                 and ol_o_id >= d_next_o_id - 20 
                 and ol_o_id < d_next_o_id 
                where d_w_id = ? and d_id = ? 
        ) 
    ) as l;

-- #deliveryBGSelectOldestNewOrder
select no_o_id 
    from bmsql_new_order 
    where no_w_id = ? and no_d_id = ? 
    order by no_o_id;

-- #deliveryBGDeleteOldestNewOrder
delete from bmsql_new_order 
    where no_w_id = ? and no_d_id = ? and no_o_id = ?;

-- #deliveryBGSelectOrder
select o_c_id 
    from bmsql_oorder 
    where o_w_id = ? and o_d_id = ? and o_id = ?;

-- #deliveryBGUpdateOrder
update bmsql_oorder 
    set o_carrier_id = ? 
    where o_w_id = ? and o_d_id = ? and o_id = ?;

-- #deliveryBGSelectSumOLAmount
select sum(ol_amount) as sum_ol_amount 
    from bmsql_order_line 
    where ol_w_id = ? and ol_d_id = ? and ol_o_id = ?;

-- #deliveryBGUpdateOrderLine
update bmsql_order_line 
    set ol_delivery_d = ? 
    where ol_w_id = ? and ol_d_id = ? and ol_o_id = ?;

-- #deliveryBGUpdateCustomer
update bmsql_customer 
    set c_balance = c_balance + ?, 
        c_delivery_cnt = c_delivery_cnt + 1 
    where c_w_id = ? and c_d_id = ? and c_id = ?;