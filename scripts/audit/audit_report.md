# Capstone Data Pipeline Audit Report (v2)

_Generated: 2026-04-28 12:05:00_

## Summary

| Layer | Logical tables | Total rows | Total size MB |
|---|---|---|---|
| RAW (client-provided) | 4 | 116,895,089 | 3378.2 |
| CLEAN (intermediate) | 5 | 224,046,266 | 12299.2 |
| PRECOMPUTED (serving) | 1 | 26,175,039 | 434.2 |

## Logical tables by layer

### RAW (client-provided)

| Table | Shards | Rows | Cols | Size MB |
|---|---|---|---|---|
| `v_dim_cust_curr_revised` | 16 | 1,134,418 | 160 | 192.05 |
| `v_dim_item_curr_revised` | 16 | 277,203 | 125 | 34.31 |
| `v_fct_sales_2425_revised` | 115 | 57,755,638 | 14 | 1510.52 |
| `v_fct_sales_2526_revised` | 122 | 57,727,830 | 14 | 1641.37 |

### CLEAN (intermediate)

| Table | Shards | Rows | Cols | Size MB |
|---|---|---|---|---|
| `customers_clean` | 1 | 1,134,418 | 17 | 46.74 |
| `features` | 3 | 778,720 | 53 | 27.69 |
| `merged_dataset` | 1 | 110,402,862 | 38 | 7458.05 |
| `products_clean` | 1 | 277,203 | 21 | 14.72 |
| `sales` | 4 | 111,453,063 | 17 | 4752.02 |

### PRECOMPUTED (serving)

| Table | Shards | Rows | Cols | Size MB |
|---|---|---|---|---|
| `precomputed` | 14 | 26,175,039 | 8 | 434.2 |

## Derivation Analysis

Compares column names across layers. Auto-detection by name match — renamed columns may show up as 'derived' even if they're really cleaned versions of raw columns. Always cross-check with feature engineering scripts before defense.

### RAW columns (extracted from client data): 291 unique

`actv_flg`, `addrss_line1`, `addrss_line2`, `addrss_line3`, `addrss_line4`, `alt_ctlg_num`, `alt_item_allow_flg`, `ar_bus_pltfrm`, `ar_caller_cd`, `ar_caller_dsc`, `atomic_to_prmry_divisor`, `atomic_uom`, `audit_crte_dt`, `audit_crte_id`, `audit_upd_dt`, `audit_upd_id`, `baa_flg`, `batch_qty_multiple`, `bid_num`, `bill_to_cust_num`, `biomed_acct_flg`, `bkordr_allow_flg`, `bus_pltfrm`, `buy_uom`, `by_trans_mode`, `by_uom_conv_to_parnt`, `ccs_cd`, `ccs_dsc`, `ccs_dt`, `ccs_e1_cd`, `city`, `cntry_cd`, `cnvrsn_dt`, `cnvrsn_type_cd`, `cnvrsn_type_dsc`, `collctn_mgr_cd`, `collctn_mgr_dsc`, `comp_ctgry_cd`, `comp_ctgry_dsc`, `corp_low_uom`, `crdt_limit_amt`, `crdt_mgr_cd`, `crdt_mgr_dsc`, `ctgry_cd`, `ctgry_dsc`, `ctlg_num`, `cust_ar_parnt_num`, `cust_choice_rwrd_elgbl_flg`, `cust_choice_rwrd_enrlmnt_flg`, `cust_e1_num`, `cust_glbl_lctn_num`, `cust_name`, `cust_num`, `cust_spclty_org_cd`, `cust_spclty_org_dsc`, `cust_suppld_acct_num`, `cust_type_cd`, `cust_type_dsc`, `cyp_brnd_cd`, `cyp_cust_lgcy_num`, `dea_lic_expr_dt`, `dea_lic_num`, `dead_net_cost_amt`, `default_e1_email_addrss`, `dim_acct_mgr_curr_id`, `dim_bill_to_cust_curr_id`, `dim_cmpny_id`, `dim_cust_ar_parnt_curr_id`, `dim_cust_ar_profile_id`, `dim_cust_curr_id`, `dim_cust_grp_id`, `dim_cust_prc_grp_lpg_id`, `dim_cust_super_grp_id`, `dim_frght_sched_id`, `dim_iss_analyst_id`, `dim_item_e1_curr_id`, `dim_ordr_dt_id`, `dim_prmry_gpo_id`, `dim_prmry_ship_dstrbtn_cntr_id`, `dim_pymnt_instrmnt_type_id`, `dim_pymnt_terms_id`, `dim_suplr_abbrv_id`, `dim_suplr_curr_id`, `disctnd_dt`, `disctnd_flg`, `dscsa_item_flg`, `dup_po_flg`, `ec_acct_specif_bid_cd`, `ec_acct_type_cd`, `ec_buy_plan_cd`, `ec_buying_grp_name`, `ec_buying_grp_num`, `ec_hndlng_cd`, `ec_promo_bid_cd`, `ec_scndry_bid_cd`, `ec_spr_grp_name`, `ec_spr_grp_num`, `ec_vip_cost_amt`, `ecomm_mkt_place_acct_flg`, `ecomm_pure_play_acct_flg`, `ecomm_web_retail_cd`, `ecomm_web_retail_dsc`, `edi_810_inv_flg`, `edi_850_po_flg`, `edi_855_po_ack_flg`, `edi_856_ship_notice_flg`, `edi_trdng_prtnr_id`, `excess_item_cd`, `fed_hzrd_cd`, `fed_legend_cd`, `flu_prebook_flg`, `frght_flg`, `frmlry_ctlg_cd`, `frmlry_ctlg_dsc`, `fuel_surchrg_flg`, `gcn_dose_frm`, `gcn_num`, `ghx_major_cd`, `ghx_major_dsc`, `ghx_match_level_cd`, `ghx_minor_cd`, `ghx_minor_dsc`, `gl_class_cd`, `gnrc_flg`, `govt_class_cd`, `govt_class_dsc`, `govt_country_of_origin_flg`, `govt_gotit_cntrct_num`, `govt_reslr_flg`, `grp_admin_name`, `gs_mgr_id`, `hc_dlvry_cd`, `hc_dlvry_dsc`, `high_rows_per_pallet`, `hlth_indstry_num`, `hpis_sku`, `inbnd_pedigree_cd`, `intrcmpny_flg`, `inv_consldtn_day_cnt`, `inv_num`, `item_dsc`, `item_e1_num`, `item_risk_score_cd`, `item_risk_score_dsc`, `item_status_cd`, `item_status_dsc`, `item_type_cd`, `last_pymnt_appld_amt`, `last_pymnt_appld_dt`, `lct_reqd_flg`, `legend_cd`, `lgcy_pltfrm`, `long_addrss_num`, `mac_cost_amt`, `mck_flg`, `mdm_guid_id`, `mdm_party_id`, `med_lic_expr_dt`, `med_lic_num`, `med_lic_state_cd`, `mfg_bkordr_dsc`, `mfg_bkordr_due_dt`, `mfg_bkordr_flg`, `mfg_bkordr_rel_dt`, `mfg_direct_cd`, `mfg_status_cd`, `mfg_status_dsc`, `min_batch_qty`, `min_ordr_chrg_exmpt_dt`, `min_ordr_chrg_exmpt_flg`, `mkt_cd`, `mktng_prgm_cd`, `mktng_prgm_dsc`, `mms_class_cd`, `mms_class_dsc`, `mms_sgmnt_cd`, `mms_sub_class_cd`, `mms_sub_class_dsc`, `mstr_grp_admin_name`, `mstr_grp_cd`, `mstr_grp_name`, `mstr_grp_num`, `mstr_grp_type_cd`, `ndc_num`, `netwrk_velcty_cd`, `netwrk_velcty_dsc`, `nrctc_cd`, `obslt_dt`, `ordr_line_num`, `ordr_max_limit_amt`, `ordr_mthd_dsc`, `ordr_num`, `ordr_qty`, `ordr_src_dsc`, `otc_flg`, `overpack_flg`, `parnt_item_num`, `parnt_suplr_name`, `parnt_suplr_num`, `parnt_suplr_srch_type`, `partial_shipmnt_allow_flg`, `pharma_acct_num`, `pharma_item_num`, `pharma_xref_num`, `phone_num`, `ppe_flg`, `prca_num`, `prime_acct_flg`, `print_crmemo_flg`, `prmry_affln_gpo_cost_flg`, `prmry_qty`, `prmry_to_atomic_divisor`, `prmry_to_buy_divisor`, `prmry_to_sell_divisor`, `prmry_uom`, `prmry_uom_wght`, `prod_class_cd`, `prod_ctgry_ec_fin_rollup_dsc`, `prod_ctgry_lab_fin_rollup_dsc`, `prod_ctgry_lvl2_cd`, `prod_ctgry_lvl2_dsc`, `prod_ctgry_pc_fin_rollup_cd`, `prod_ctgry_pc_fin_rollup_dsc`, `prod_fmly_lvl1_cd`, `prod_fmly_lvl1_dsc`, `prod_grp_cd`, `prod_grp_dsc`, `prod_grp_lvl3_cd`, `prod_grp_lvl3_dsc`, `prod_sub_ctgry_lvl4_cd`, `prod_sub_ctgry_lvl4_dsc`, `prod_sub_grp_cd`, `prod_sub_grp_dsc`, `prod_type_cd`, `prod_type_dsc`, `prvt_brnd_flg`, `ptnt_flg`, `ptnt_pack_bulk_ship_flg`, `pymnt_terms_ec_cd`, `pymnt_terms_ec_dsc`, `rpt_naming_cd`, `rx_gpo_excl_type_cd`, `rx_gpo_excl_type_dsc`, `sell_corp_acq_cost_amt`, `sell_to_buy_divisor`, `sell_uom`, `ship_qty`, `shrd_acct_flg`, `singl_gso_rstrctn_flg`, `sister_340b_acct_num`, `sister_340b_type_cd`, `sister_340b_type_dsc`, `site_addrss_state`, `sls_grp_cd`, `sls_grp_dsc`, `sort_seq_num`, `spclty_cd`, `spclty_dsc`, `spclty_rx_flg`, `srvc_ctr_cd`, `srvc_ctr_dsc`, `start_dt`, `state`, `stndrd_cost_amt`, `stock_type_cd`, `strg_cd`, `strg_type_cd`, `strg_type_dsc`, `sub_ctgry_cd`, `sub_ctgry_dsc`, `super_grp_admin_name`, `suplr_abbrv`, `suplr_dsc`, `suplr_rollup_dsc`, `sys_pltfrm`, `t_ordr_cd`, `t_ordr_dsc`, `taa_flg`, `tariff_cd`, `tax_cd`, `tax_status_cd`, `tax_status_dsc`, `tie_cases_per_row`, `tier_cd`, `tier_dsc`, `traceable_type`, `unit_sls_amt`, `unspsc_cd`, `unspsc_dsc`, `unspsc_vrsn`, `zip`

### Raw columns USED in clean layer: 46

`actv_flg`, `city`, `cntry_cd`, `cust_name`, `cust_num`, `cust_type_cd`, `cust_type_dsc`, `dim_cust_curr_id`, `dim_item_e1_curr_id`, `dim_ordr_dt_id`, `disctnd_flg`, `gnrc_flg`, `inv_num`, `item_dsc`, `item_e1_num`, `mck_flg`, `mkt_cd`, `mms_class_cd`, `mms_class_dsc`, `mms_sgmnt_cd`, `mms_sub_class_cd`, `ordr_line_num`, `ordr_mthd_dsc`, `ordr_num`, `ordr_qty`, `ordr_src_dsc`, `prmry_qty`, `prod_ctgry_lvl2_cd`, `prod_ctgry_lvl2_dsc`, `prod_fmly_lvl1_cd`, `prod_fmly_lvl1_dsc`, `prod_grp_lvl3_cd`, `prod_grp_lvl3_dsc`, `prod_sub_ctgry_lvl4_cd`, `prod_sub_ctgry_lvl4_dsc`, `prvt_brnd_flg`, `ship_qty`, `sls_grp_cd`, `sls_grp_dsc`, `spclty_cd`, `spclty_dsc`, `state`, `suplr_dsc`, `suplr_rollup_dsc`, `unit_sls_amt`, `zip`

### Raw columns USED in precomputed layer: 2

`dim_cust_curr_id`, `dim_item_e1_curr_id`

### Raw columns DROPPED (not in clean or precomputed): 245

`addrss_line1`, `addrss_line2`, `addrss_line3`, `addrss_line4`, `alt_ctlg_num`, `alt_item_allow_flg`, `ar_bus_pltfrm`, `ar_caller_cd`, `ar_caller_dsc`, `atomic_to_prmry_divisor`, `atomic_uom`, `audit_crte_dt`, `audit_crte_id`, `audit_upd_dt`, `audit_upd_id`, `baa_flg`, `batch_qty_multiple`, `bid_num`, `bill_to_cust_num`, `biomed_acct_flg`, `bkordr_allow_flg`, `bus_pltfrm`, `buy_uom`, `by_trans_mode`, `by_uom_conv_to_parnt`, `ccs_cd`, `ccs_dsc`, `ccs_dt`, `ccs_e1_cd`, `cnvrsn_dt`, `cnvrsn_type_cd`, `cnvrsn_type_dsc`, `collctn_mgr_cd`, `collctn_mgr_dsc`, `comp_ctgry_cd`, `comp_ctgry_dsc`, `corp_low_uom`, `crdt_limit_amt`, `crdt_mgr_cd`, `crdt_mgr_dsc`, `ctgry_cd`, `ctgry_dsc`, `ctlg_num`, `cust_ar_parnt_num`, `cust_choice_rwrd_elgbl_flg`, `cust_choice_rwrd_enrlmnt_flg`, `cust_e1_num`, `cust_glbl_lctn_num`, `cust_spclty_org_cd`, `cust_spclty_org_dsc`, `cust_suppld_acct_num`, `cyp_brnd_cd`, `cyp_cust_lgcy_num`, `dea_lic_expr_dt`, `dea_lic_num`, `dead_net_cost_amt`, `default_e1_email_addrss`, `dim_acct_mgr_curr_id`, `dim_bill_to_cust_curr_id`, `dim_cmpny_id`, `dim_cust_ar_parnt_curr_id`, `dim_cust_ar_profile_id`, `dim_cust_grp_id`, `dim_cust_prc_grp_lpg_id`, `dim_cust_super_grp_id`, `dim_frght_sched_id`, `dim_iss_analyst_id`, `dim_prmry_gpo_id`, `dim_prmry_ship_dstrbtn_cntr_id`, `dim_pymnt_instrmnt_type_id`, `dim_pymnt_terms_id`, `dim_suplr_abbrv_id`, `dim_suplr_curr_id`, `disctnd_dt`, `dscsa_item_flg`, `dup_po_flg`, `ec_acct_specif_bid_cd`, `ec_acct_type_cd`, `ec_buy_plan_cd`, `ec_buying_grp_name`, `ec_buying_grp_num`, `ec_hndlng_cd`, `ec_promo_bid_cd`, `ec_scndry_bid_cd`, `ec_spr_grp_name`, `ec_spr_grp_num`, `ec_vip_cost_amt`, `ecomm_mkt_place_acct_flg`, `ecomm_pure_play_acct_flg`, `ecomm_web_retail_cd`, `ecomm_web_retail_dsc`, `edi_810_inv_flg`, `edi_850_po_flg`, `edi_855_po_ack_flg`, `edi_856_ship_notice_flg`, `edi_trdng_prtnr_id`, `excess_item_cd`, `fed_hzrd_cd`, `fed_legend_cd`, `flu_prebook_flg`, `frght_flg`, `frmlry_ctlg_cd`, `frmlry_ctlg_dsc`, `fuel_surchrg_flg`, `gcn_dose_frm`, `gcn_num`, `ghx_major_cd`, `ghx_major_dsc`, `ghx_match_level_cd`, `ghx_minor_cd`, `ghx_minor_dsc`, `gl_class_cd`, `govt_class_cd`, `govt_class_dsc`, `govt_country_of_origin_flg`, `govt_gotit_cntrct_num`, `govt_reslr_flg`, `grp_admin_name`, `gs_mgr_id`, `hc_dlvry_cd`, `hc_dlvry_dsc`, `high_rows_per_pallet`, `hlth_indstry_num`, `hpis_sku`, `inbnd_pedigree_cd`, `intrcmpny_flg`, `inv_consldtn_day_cnt`, `item_risk_score_cd`, `item_risk_score_dsc`, `item_status_cd`, `item_status_dsc`, `item_type_cd`, `last_pymnt_appld_amt`, `last_pymnt_appld_dt`, `lct_reqd_flg`, `legend_cd`, `lgcy_pltfrm`, `long_addrss_num`, `mac_cost_amt`, `mdm_guid_id`, `mdm_party_id`, `med_lic_expr_dt`, `med_lic_num`, `med_lic_state_cd`, `mfg_bkordr_dsc`, `mfg_bkordr_due_dt`, `mfg_bkordr_flg`, `mfg_bkordr_rel_dt`, `mfg_direct_cd`, `mfg_status_cd`, `mfg_status_dsc`, `min_batch_qty`, `min_ordr_chrg_exmpt_dt`, `min_ordr_chrg_exmpt_flg`, `mktng_prgm_cd`, `mktng_prgm_dsc`, `mms_sub_class_dsc`, `mstr_grp_admin_name`, `mstr_grp_cd`, `mstr_grp_name`, `mstr_grp_num`, `mstr_grp_type_cd`, `ndc_num`, `netwrk_velcty_cd`, `netwrk_velcty_dsc`, `nrctc_cd`, `obslt_dt`, `ordr_max_limit_amt`, `otc_flg`, `overpack_flg`, `parnt_item_num`, `parnt_suplr_name`, `parnt_suplr_num`, `parnt_suplr_srch_type`, `partial_shipmnt_allow_flg`, `pharma_acct_num`, `pharma_item_num`, `pharma_xref_num`, `phone_num`, `ppe_flg`, `prca_num`, `prime_acct_flg`, `print_crmemo_flg`, `prmry_affln_gpo_cost_flg`, `prmry_to_atomic_divisor`, `prmry_to_buy_divisor`, `prmry_to_sell_divisor`, `prmry_uom`, `prmry_uom_wght`, `prod_class_cd`, `prod_ctgry_ec_fin_rollup_dsc`, `prod_ctgry_lab_fin_rollup_dsc`, `prod_ctgry_pc_fin_rollup_cd`, `prod_ctgry_pc_fin_rollup_dsc`, `prod_grp_cd`, `prod_grp_dsc`, `prod_sub_grp_cd`, `prod_sub_grp_dsc`, `prod_type_cd`, `prod_type_dsc`, `ptnt_flg`, `ptnt_pack_bulk_ship_flg`, `pymnt_terms_ec_cd`, `pymnt_terms_ec_dsc`, `rpt_naming_cd`, `rx_gpo_excl_type_cd`, `rx_gpo_excl_type_dsc`, `sell_corp_acq_cost_amt`, `sell_to_buy_divisor`, `sell_uom`, `shrd_acct_flg`, `singl_gso_rstrctn_flg`, `sister_340b_acct_num`, `sister_340b_type_cd`, `sister_340b_type_dsc`, `site_addrss_state`, `sort_seq_num`, `spclty_rx_flg`, `srvc_ctr_cd`, `srvc_ctr_dsc`, `start_dt`, `stndrd_cost_amt`, `stock_type_cd`, `strg_cd`, `strg_type_cd`, `strg_type_dsc`, `sub_ctgry_cd`, `sub_ctgry_dsc`, `super_grp_admin_name`, `suplr_abbrv`, `sys_pltfrm`, `t_ordr_cd`, `t_ordr_dsc`, `taa_flg`, `tariff_cd`, `tax_cd`, `tax_status_cd`, `tax_status_dsc`, `tie_cases_per_row`, `tier_cd`, `tier_dsc`, `traceable_type`, `unspsc_cd`, `unspsc_dsc`, `unspsc_vrsn`

### NEW columns in CLEAN (not in raw): 54 candidates

`F_score`, `M_score`, `RFM_score`, `R_score`, `active_months_last_12`, `affordability_ceiling`, `avg_order_gap_days`, `avg_revenue_per_order`, `category_hhi`, `churn_label`, `cust_type_encoded`, `cycle_regularity`, `fiscal_year`, `frequency`, `is_discontinued`, `is_generic`, `is_private_brand`, `median_monthly_spend`, `mkt_cd_encoded`, `mms_class_encoded`, `monetary`, `n_categories_bought`, `order_day`, `order_month`, `order_year`, `pct_of_total_revenue`, `recency_days`, `size_tier`, `spec_CHC`, `spec_D`, `spec_EM`, `spec_FP`, `spec_GP`, `spec_GS`, `spec_HIA`, `spec_HL`, `spec_IM`, `spec_M04`, `spec_M07`, `spec_M14`, `spec_M16`, `spec_O`, `spec_OBG`, `spec_ON`, `spec_PD`, `spec_R`, `spec_SC`, `spec_SKL`, `spec_avg_revenue_per_order`, `specialty_revenue_trend_pct`, `specialty_tier`, `state_encoded`, `state_grouped`, `supplier_profile`

### NEW columns in PRECOMPUTED (not in raw): 6 candidates

`days_since_last`, `first_order_date`, `last_order_date`, `n_lines`, `total_qty`, `total_spend`

---

## Layer detail: RAW (client-provided)

### `v_dim_cust_curr_revised` _(partitioned dataset, 16 shards)_

- **Folder:** `C:\Users\maina\Desktop\Capstone\data_raw\v_dim_cust_curr_revised`
- **Total size:** 192.05 MB
- **Rows:** 1,134,418
- **Columns:** 160

**Columns:**

| # | Column | Type | Nulls | Stats |
|---|---|---|---|---|
| 1 | `DIM_CUST_CURR_ID` | DECIMAL(38,0) | 0 (0.0%) | min=32757, max=593783882, mean=152892627.2991, median=12197400 |
| 2 | `CUST_NUM` | DECIMAL(38,0) | 0 (0.0%) | min=51559, max=98871720, mean=54559318.3996, median=59122948 |
| 3 | `DIM_ACCT_MGR_CURR_ID` | DECIMAL(38,0) | 0 (0.0%) | min=-1, max=279244, mean=52869.2179, median=63467 |
| 4 | `LGCY_PLTFRM` | VARCHAR | 0 (0.0%) | distinct=1; top: `MMS` (1,134,418) |
| 5 | `SYS_PLTFRM` | VARCHAR | 0 (0.0%) | distinct=1; top: `E1` (1,134,418) |
| 6 | `BUS_PLTFRM` | VARCHAR | 0 (0.0%) | distinct=3; top: `PC` (868,321), `EC` (265,914), `N/A` (183) |
| 7 | `CUST_NAME` | VARCHAR | 0 (0.0%) | distinct=725,252; top: (skipped — high cardinality) |
| 8 | `CUST_TYPE_CD` | VARCHAR | 0 (0.0%) | distinct=4; top: `S` (860,893), `B` (219,662), `X` (53,244), `N/A` (619) |
| 9 | `CUST_TYPE_DSC` | VARCHAR | 0 (0.0%) | distinct=4; top: `SHIP TO ADDRESS ONLY` (860,893), `BILL TO ADDRESS ONLY` (219,662), `BILL TO AND SHIP TO ADDRESS` (53,244), `N/A` (619) |
| 10 | `DIM_BILL_TO_CUST_CURR_ID` | DECIMAL(38,0) | 0 (0.0%) | min=-1, max=593783710, mean=91274905.9297, median=5789933 |
| 11 | `BILL_TO_CUST_NUM` | DECIMAL(38,0) | 0 (0.0%) | min=1, max=98871719, mean=44355009.557, median=54750096 |
| 12 | `ADDRSS_LINE1` | VARCHAR | 720,644 (63.5%) | distinct=225,788; top: (skipped — high cardinality) |
| 13 | `ADDRSS_LINE2` | VARCHAR | 2,145 (0.2%) | distinct=676,931; top: (skipped — high cardinality) |
| 14 | `ADDRSS_LINE3` | VARCHAR | 1,098,947 (96.9%) | distinct=22,314; top: `BIOMED` (7,779), `WELLSPAN` (546), `ATTN:ACCTS PA` (458), `CA2684` (326), `MMCAP RX` (158) |
| 15 | `ADDRSS_LINE4` | VARCHAR | 1,126,595 (99.3%) | distinct=3,542; top: `BIOMED` (1,145), `MMCAP RX` (538), `GOWN ORDER` (517), `HOME` (376), `CREDIT INACTIVATIONS_4-9-19_MB` (284) |
| 16 | `CITY` | VARCHAR | 12 (0.0%) | distinct=14,449; top: `HOUSTON` (12,822), `NEW YORK` (7,370), `MIAMI` (7,232), `SAN ANTONIO` (6,891), `PHOENIX` (6,616) |
| 17 | `STATE` | VARCHAR | 9 (0.0%) | distinct=64; top: `CA` (117,696), `TX` (103,567), `FL` (90,690), `NY` (51,513), `PA` (50,007) |
| 18 | `ZIP` | VARCHAR | 30 (0.0%) | distinct=348,104; top: (skipped — high cardinality) |
| 19 | `PHONE_NUM` | VARCHAR | 414,726 (36.6%) | distinct=371,789; top: (skipped — high cardinality) |
| 20 | `ACTV_FLG` | VARCHAR | 0 (0.0%) | distinct=1; top: `Y` (1,134,418) |
| 21 | `DEA_LIC_EXPR_DT` | TIMESTAMP | 1,100,182 (97.0%) | min=1917-02-28 00:00:00, max=2029-05-31 00:00:00 |
| 22 | `DEA_LIC_NUM` | VARCHAR | 1,100,182 (97.0%) | distinct=30,915; top: `ST CNTRL` (191), `PER ERIN` (86), `BS2903145` (10), `AR1687081` (10), `SUSPENDED PER CRX 10/25/2022` (8) |
| 23 | `MED_LIC_EXPR_DT` | TIMESTAMP | 658,613 (58.1%) | min=1930-12-31 00:00:00, max=2107-11-30 00:00:00 |
| 24 | `MED_LIC_NUM` | VARCHAR | 658,613 (58.1%) | distinct=243,214; top: (skipped — high cardinality) |
| 25 | `MED_LIC_STATE_CD` | VARCHAR | 1,084,994 (95.6%) | distinct=54; top: `CA` (5,505), `TX` (5,364), `FL` (4,527), `MA` (2,094), `GA` (1,977) |
| 26 | `SPCLTY_DSC` | VARCHAR | 190 (0.0%) | distinct=274; top: `FAMILY PRACTICE` (101,555), `OTHER` (60,902), `SKILLED` (53,281), `HOME MEDICAL EQUIPMENT` (41,106), `MULTIPLE SPECIALTY GROUP PRACT` (41,036) |
| 27 | `START_DT` | TIMESTAMP | 619 (0.1%) | min=1980-01-01 00:00:00, max=2026-04-15 00:00:00 |
| 28 | `PTNT_FLG` | VARCHAR | 0 (0.0%) | distinct=1; top: `N` (1,134,418) |
| 29 | `SPCLTY_CD` | VARCHAR | 0 (0.0%) | distinct=278; top: `FP` (101,555), `M04` (60,902), `SKL` (53,281), `M14` (41,106), `M07` (41,036) |
| 30 | `SHRD_ACCT_FLG` | VARCHAR | 0 (0.0%) | distinct=2; top: `N` (1,134,353), `Y` (65) |
| 31 | `MKT_CD` | VARCHAR | 0 (0.0%) | distinct=8; top: `PO` (788,758), `LTC` (203,684), `HC` (61,784), `SC` (40,316), `LC` (33,486) |
| 32 | `MSTR_GRP_NUM` | DECIMAL(38,0) | 1,134,418 (100.0%) | min=None, max=None, mean=None, median=None |
| 33 | `MSTR_GRP_NAME` | VARCHAR | 0 (0.0%) | distinct=1; top: `N/A` (1,134,418) |
| 34 | `MMS_CLASS_CD` | VARCHAR | 0 (0.0%) | distinct=5; top: `B` (751,491), `D` (266,300), `G` (116,604), `A` (16), `N/A` (7) |
| 35 | `MSTR_GRP_CD` | VARCHAR | 0 (0.0%) | distinct=1; top: `N/A` (1,134,418) |
| 36 | `MMS_SUB_CLASS_CD` | VARCHAR | 0 (0.0%) | distinct=39; top: `14` (501,853), `10` (179,785), `06` (58,760), `11` (51,395), `17` (36,125) |
| 37 | `MMS_SGMNT_CD` | VARCHAR | 0 (0.0%) | distinct=13; top: `14` (788,974), `10` (187,054), `06` (61,455), `17` (40,043), `35` (33,492) |
| 38 | `MSTR_GRP_ADMIN_NAME` | VARCHAR | 0 (0.0%) | distinct=1; top: `N/A` (1,134,418) |
| 39 | `MSTR_GRP_TYPE_CD` | VARCHAR | 0 (0.0%) | distinct=1; top: `N/A` (1,134,418) |
| 40 | `RPT_NAMING_CD` | VARCHAR | 0 (0.0%) | distinct=1; top: `N/A` (1,134,418) |
| 41 | `GOVT_CLASS_DSC` | VARCHAR | 0 (0.0%) | distinct=12; top: `NOT USED` (914,794), `COMMERCIAL` (78,667), `STATE GOVT` (59,285), `LOCAL GOVT` (36,330), `GOV CONTRACTOR STATE/LOCAL` (14,027) |
| 42 | `INTRCMPNY_FLG` | VARCHAR | 0 (0.0%) | distinct=2; top: `N` (1,123,257), `Y` (11,161) |
| 43 | `CUST_E1_NUM` | DECIMAL(38,0) | 0 (0.0%) | min=51559, max=98871720, mean=54559318.3996, median=59122948 |
| 44 | `CCS_DT` | TIMESTAMP | 1,134,418 (100.0%) | min=None, max=None |
| 45 | `CCS_CD` | VARCHAR | 1,134,418 (100.0%) | distinct=0; top:  |
| 46 | `CCS_DSC` | VARCHAR | 1,134,418 (100.0%) | distinct=0; top:  |
| 47 | `DIM_PRMRY_GPO_ID` | DECIMAL(38,0) | 0 (0.0%) | min=-1, max=3822485, mean=1246761.9113, median=-1 |
| 48 | `CCS_E1_CD` | VARCHAR | 1,056,916 (93.2%) | distinct=1; top: `P` (77,502) |
| 49 | `DIM_PRMRY_SHIP_DSTRBTN_CNTR_ID` | DECIMAL(38,0) | 0 (0.0%) | min=0, max=5040, mean=2587.0601, median=2658 |
| 50 | `DIM_CUST_AR_PARNT_CURR_ID` | DECIMAL(38,0) | 0 (0.0%) | min=-1, max=593761678, mean=10616652.3493, median=-1 |
| 51 | `PYMNT_TERMS_EC_CD` | VARCHAR | 0 (0.0%) | distinct=1; top: `N/A` (1,134,418) |
| 52 | `PYMNT_TERMS_EC_DSC` | VARCHAR | 0 (0.0%) | distinct=1; top: `N/A` (1,134,418) |
| 53 | `BID_NUM` | VARCHAR | 0 (0.0%) | distinct=1; top: `N/A` (1,134,418) |
| 54 | `CUST_AR_PARNT_NUM` | DECIMAL(38,0) | 0 (0.0%) | min=0, max=98853840, mean=10366148.0563, median=0 |
| 55 | `HC_DLVRY_CD` | VARCHAR | 0 (0.0%) | distinct=4; top: `N/A` (876,144), `BLK` (226,433), `PHD` (28,744), `HD` (3,097) |
| 56 | `HC_DLVRY_DSC` | VARCHAR | 0 (0.0%) | distinct=4; top: `BLANK` (876,144), `BULK DELIVERY` (226,433), `PATIENT HOME DELIVERY` (28,744), `HOME DELIVERY` (3,097) |
| 57 | `SITE_ADDRSS_STATE` | VARCHAR | 0 (0.0%) | distinct=1; top: `N/A` (1,134,418) |
| 58 | `BKORDR_ALLOW_FLG` | VARCHAR | 0 (0.0%) | distinct=2; top: `Y` (1,132,320), `N` (2,098) |
| 59 | `GOVT_RESLR_FLG` | VARCHAR | 0 (0.0%) | distinct=2; top: `N` (1,124,118), `Y` (10,300) |
| 60 | `CRDT_LIMIT_AMT` | DECIMAL(38,10) | 619 (0.1%) | min=0E-10, max=99999999999.0000000000, mean=250034.4532, median=1.0000000000 |
| 61 | `AR_CALLER_CD` | VARCHAR | 2 (0.0%) | distinct=185; top: `144` (56,362), `143` (45,043), `148` (44,908), `969` (39,772), `752` (32,477) |
| 62 | `AR_CALLER_DSC` | VARCHAR | 632 (0.1%) | distinct=118; top: `WALDRUM,M` (56,577), `ALEXANDER` (46,111), `MCNEAL,D` (45,038), `ISAACS,M` (44,908), `PERKINS,C` (39,772) |
| 63 | `AR_BUS_PLTFRM` | VARCHAR | 0 (0.0%) | distinct=2; top: `PC` (787,934), `EC` (346,484) |
| 64 | `CNVRSN_TYPE_CD` | VARCHAR | 0 (0.0%) | distinct=7; top: `N/A` (783,860), `E` (252,447), `MM` (84,336), `MSD` (13,269), `L` (483) |
| 65 | `CNVRSN_TYPE_DSC` | VARCHAR | 0 (0.0%) | distinct=7; top: `N/A` (783,860), `EC CUSTOMER MIGRATION` (252,447), `MOORE MEDICAL CUSTOMER` (84,336), `MSD CUSTOMER` (13,269), `LABSCO` (483) |
| 66 | `EC_BUYING_GRP_NUM` | DECIMAL(38,0) | 1,134,418 (100.0%) | min=None, max=None, mean=None, median=None |
| 67 | `EC_BUYING_GRP_NAME` | VARCHAR | 0 (0.0%) | distinct=1; top: `N/A` (1,134,418) |
| 68 | `EC_SCNDRY_BID_CD` | VARCHAR | 0 (0.0%) | distinct=1; top: `N/A` (1,134,418) |
| 69 | `EC_ACCT_SPECIF_BID_CD` | VARCHAR | 0 (0.0%) | distinct=1; top: `N/A` (1,134,418) |
| 70 | `EC_PROMO_BID_CD` | VARCHAR | 0 (0.0%) | distinct=1; top: `N/A` (1,134,418) |
| 71 | `EC_BUY_PLAN_CD` | VARCHAR | 0 (0.0%) | distinct=1; top: `N/A` (1,134,418) |
| 72 | `EC_ACCT_TYPE_CD` | VARCHAR | 0 (0.0%) | distinct=1; top: `N/A` (1,134,418) |
| 73 | `EC_SPR_GRP_NUM` | DECIMAL(38,0) | 1,134,418 (100.0%) | min=None, max=None, mean=None, median=None |
| 74 | `EC_SPR_GRP_NAME` | VARCHAR | 0 (0.0%) | distinct=1; top: `N/A` (1,134,418) |
| 75 | `GOVT_CLASS_CD` | VARCHAR | 0 (0.0%) | distinct=12; top: `N/A` (914,794), `X` (78,667), `S` (59,285), `L` (36,330), `W` (14,027) |
| 76 | `TIER_CD` | VARCHAR | 0 (0.0%) | distinct=10; top: `N/A` (963,580), `T2` (39,828), `T3` (36,453), `T1` (36,331), `T4` (32,796) |
| 77 | `TIER_DSC` | VARCHAR | 0 (0.0%) | distinct=10; top: `N/A` (963,580), `CORPORATE` (39,828), `INDEPENDENT` (36,453), `STRATEGIC` (36,331), `INSIDE SALES` (32,796) |
| 78 | `ECOMM_WEB_RETAIL_CD` | VARCHAR | 0 (0.0%) | distinct=3; top: `N/A` (1,129,398), `ECOM1` (5,018), `*` (2) |
| 79 | `ECOMM_WEB_RETAIL_DSC` | VARCHAR | 0 (0.0%) | distinct=3; top: `N/A` (1,129,398), `ECOM B2B2C` (5,018), `*` (2) |
| 80 | `CNVRSN_DT` | TIMESTAMP | 994,303 (87.6%) | min=1900-01-01 00:00:00, max=2099-12-31 00:00:00 |
| 81 | `GOVT_COUNTRY_OF_ORIGIN_FLG` | VARCHAR | 0 (0.0%) | distinct=4; top: `U` (1,121,889), `Y` (10,300), `N` (2,227), `*` (2) |
| 82 | `MIN_ORDR_CHRG_EXMPT_FLG` | VARCHAR | 0 (0.0%) | distinct=3; top: `N` (908,816), `Y` (225,600), `*` (2) |
| 83 | `MIN_ORDR_CHRG_EXMPT_DT` | TIMESTAMP | 732,893 (64.6%) | min=1900-01-01 00:00:00, max=2199-12-31 00:00:00 |
| 84 | `FUEL_SURCHRG_FLG` | VARCHAR | 0 (0.0%) | distinct=3; top: `Y` (665,035), `N` (469,381), `*` (2) |
| 85 | `T_ORDR_CD` | VARCHAR | 0 (0.0%) | distinct=7; top: `F` (1,074,809), `Y` (26,838), `N` (25,435), `T` (5,083), `R` (1,961) |
| 86 | `T_ORDR_DSC` | VARCHAR | 0 (0.0%) | distinct=7; top: `ALT SOURCG ALLOWED FRT EXEMPT` (1,074,809), `NO ALTERNATE SOURCING ALLOWED` (26,838), `CUSTOMER ACCEPTS ALL ORDERS` (25,435), `NO AUTO T-ORDERS ALLOWED` (5,083), `REGIONAL SOURCING ONLY ALLOWED` (1,961) |
| 87 | `ALT_ITEM_ALLOW_FLG` | VARCHAR | 0 (0.0%) | distinct=4; top: `N` (870,401), `Y` (263,398), `U` (617), `*` (2) |
| 88 | `PARTIAL_SHIPMNT_ALLOW_FLG` | VARCHAR | 0 (0.0%) | distinct=4; top: `Y` (1,131,331), `N` (2,468), `U` (617), `*` (2) |
| 89 | `DIM_CUST_GRP_ID` | DECIMAL(38,0) | 0 (0.0%) | min=-1, max=158666, mean=20262.2811, median=-1 |
| 90 | `GRP_ADMIN_NAME` | VARCHAR | 0 (0.0%) | distinct=222; top: `N/A` (948,453), `THOMPSON, GARY` (29,683), `MASSEY, THOMAS` (28,274), `DURKIN, KEVIN` (8,380), `BAYS, CARRIE` (8,236) |
| 91 | `DIM_CUST_SUPER_GRP_ID` | DECIMAL(38,0) | 0 (0.0%) | min=-1, max=158666, mean=20323.6926, median=-1 |
| 92 | `SUPER_GRP_ADMIN_NAME` | VARCHAR | 0 (0.0%) | distinct=222; top: `N/A` (948,160), `THOMPSON, GARY` (29,683), `MASSEY, THOMAS` (28,274), `BAYS, CARRIE` (8,236), `DURKIN, KEVIN` (8,033) |
| 93 | `MDM_PARTY_ID` | VARCHAR | 1,134,418 (100.0%) | distinct=0; top:  |
| 94 | `MDM_GUID_ID` | VARCHAR | 1,134,418 (100.0%) | distinct=0; top:  |
| 95 | `DIM_PYMNT_TERMS_ID` | DECIMAL(38,0) | 0 (0.0%) | min=0, max=1210, mean=264.622, median=372 |
| 96 | `DIM_PYMNT_INSTRMNT_TYPE_ID` | DECIMAL(38,0) | 0 (0.0%) | min=12, max=40, mean=27.8617, median=28 |
| 97 | `DIM_ISS_ANALYST_ID` | DECIMAL(38,0) | 0 (0.0%) | min=-1, max=203, mean=163.1902, median=199 |
| 98 | `DIM_CUST_AR_PROFILE_ID` | DECIMAL(38,0) | 0 (0.0%) | min=0, max=169496, mean=28145.8401, median=16538 |
| 99 | `DIM_FRGHT_SCHED_ID` | DECIMAL(38,0) | 0 (0.0%) | min=-1, max=11839, mean=2073.2109, median=1374 |
| 100 | `LONG_ADDRSS_NUM` | VARCHAR | 0 (0.0%) | distinct=1,134,275; top: (skipped — high cardinality) |
| 101 | `ORDR_MAX_LIMIT_AMT` | DECIMAL(38,10) | 619 (0.1%) | min=0E-10, max=9999999999.0000000000, mean=766711.5274, median=1999.0000000000 |
| 102 | `LAST_PYMNT_APPLD_DT` | TIMESTAMP | 808,615 (71.3%) | min=1985-07-29 00:00:00, max=2026-04-15 00:00:00 |
| 103 | `LAST_PYMNT_APPLD_AMT` | DECIMAL(38,10) | 619 (0.1%) | min=-6768201.1900000000, max=20764.8500000000, mean=-692.2167, median=0E-10 |
| 104 | `INV_CONSLDTN_DAY_CNT` | DECIMAL(38,10) | 224 (0.0%) | min=0E-10, max=5.0000000000, mean=0.1058, median=0E-10 |
| 105 | `PHARMA_ACCT_NUM` | DECIMAL(38,0) | 0 (0.0%) | min=0, max=89657403, mean=6666.891, median=0 |
| 106 | `DEFAULT_E1_EMAIL_ADDRSS` | VARCHAR | 1,260 (0.1%) | distinct=165,763; top: (skipped — high cardinality) |
| 107 | `MFG_DIRECT_CD` | VARCHAR | 0 (0.0%) | distinct=3; top: `N/A` (1,132,499), `SIEMENS` (1,133), `BMX` (786) |
| 108 | `FRGHT_FLG` | VARCHAR | 2 (0.0%) | distinct=4; top: `N` (786,782), `Y` (347,612), `!` (21), `n` (1) |
| 109 | `PRCA_NUM` | DECIMAL(38,0) | 0 (0.0%) | min=0, max=98871719, mean=43182380.4613, median=54567535 |
| 110 | `FRMLRY_CTLG_CD` | VARCHAR | 2 (0.0%) | distinct=1,791; top: `N/A` (989,271), `RAINBOW` (25,418), `RESTRICTED` (23,655), `SEVITA` (6,088), `DPP` (4,907) |
| 111 | `FRMLRY_CTLG_DSC` | VARCHAR | 2 (0.0%) | distinct=1,787; top: `N/A` (989,271), `Rainbow Kinder Care` (25,418), `MMCAP` (23,655), `Sevita` (6,088), `DPP` (4,907) |
| 112 | `MKTNG_PRGM_CD` | VARCHAR | 2 (0.0%) | distinct=9; top: `N/A` (1,123,367), `MT` (3,434), `E` (3,155), `V` (2,860), `V2` (1,028) |
| 113 | `MKTNG_PRGM_DSC` | VARCHAR | 2 (0.0%) | distinct=6; top: `N/A` (1,123,374), `Previously on the Momentum program` (3,434), `Existing. Already with MMS, but new to the program` (3,155), `Previosly on the VIP Gold program` (2,860), `Previosly on the VIP Gold Level 2 program` (1,028) |
| 114 | `EC_HNDLNG_CD` | VARCHAR | 0 (0.0%) | distinct=1; top: `N/A` (1,134,418) |
| 115 | `GOVT_GOTIT_CNTRCT_NUM` | VARCHAR | 1,122,110 (98.9%) | distinct=56; top: `696` (8,555), `4393` (1,550), `N` (301), `7146` (230), `8951` (192) |
| 116 | `TAX_STATUS_CD` | VARCHAR | 2 (0.0%) | distinct=26; top: `T` (794,357), `E` (326,535), `F` (8,090), `M55` (1,592), `L` (1,059) |
| 117 | `TAX_STATUS_DSC` | VARCHAR | 2 (0.0%) | distinct=26; top: `TAXABLE` (794,357), `EXEMPT` (326,535), `FEDERAL GOVERNMENT` (8,090), `WA BOX 1,2, NOT 3, 14 PLUS` (1,592), `LOCAL` (1,059) |
| 118 | `CUST_SUPPLD_ACCT_NUM` | VARCHAR | 2 (0.0%) | distinct=12,888; top: `N/A` (1,111,315), `B129758` (91), `2000` (63), `HOSPICE` (63), `B110968` (44) |
| 119 | `PHARMA_XREF_NUM` | VARCHAR | 0 (0.0%) | distinct=333,545; top: (skipped — high cardinality) |
| 120 | `HLTH_INDSTRY_NUM` | VARCHAR | 0 (0.0%) | distinct=269,504; top: (skipped — high cardinality) |
| 121 | `PRMRY_AFFLN_GPO_COST_FLG` | VARCHAR | 0 (0.0%) | distinct=6; top: `N` (938,462), `Y` (179,403), `Z` (15,688), `X` (566), `L` (297) |
| 122 | `SINGL_GSO_RSTRCTN_FLG` | VARCHAR | 0 (0.0%) | distinct=3; top: `N` (1,107,894), `Y` (26,522), `*` (2) |
| 123 | `PRIME_ACCT_FLG` | VARCHAR | 0 (0.0%) | distinct=3; top: `N` (1,134,378), `Y` (38), `*` (2) |
| 124 | `SISTER_340B_ACCT_NUM` | DECIMAL(38,0) | 0 (0.0%) | min=0, max=98853840, mean=611561.7218, median=0 |
| 125 | `SISTER_340B_TYPE_CD` | VARCHAR | 0 (0.0%) | distinct=6; top: `NA` (1,110,054), `N/A` (11,511), `APEXUS` (5,986), `WAC` (5,811), `340B` (1,054) |
| 126 | `SISTER_340B_TYPE_DSC` | VARCHAR | 0 (0.0%) | distinct=4; top: `UNKNOWN` (1,111,110), `N/A` (11,511), `340B SUBGROUP` (5,986), `WHOLESALE ACQ COST ACCT` (5,811) |
| 127 | `RX_GPO_EXCL_TYPE_CD` | VARCHAR | 0 (0.0%) | distinct=4; top: `N/A` (1,133,814), `DSH` (588), `PED` (14), `*` (2) |
| 128 | `RX_GPO_EXCL_TYPE_DSC` | VARCHAR | 0 (0.0%) | distinct=4; top: `N/A` (1,133,814), `DISPROPORTIONATE SHARE HOSPITAL` (588), `CHILDERNS HOSPITAL` (14), `UNKNOWN` (2) |
| 129 | `SRVC_CTR_CD` | VARCHAR | 0 (0.0%) | distinct=13; top: `N/A` (613,843), `DET` (100,521), `FAR` (77,089), `ATL` (67,652), `ONT` (58,390) |
| 130 | `SRVC_CTR_DSC` | VARCHAR | 0 (0.0%) | distinct=26; top: `N/A` (526,052), `DETROIT CSC` (72,553), `HOME CARE SC` (50,352), `ATLANTA CUSTOMER SERVICE CENTER` (49,086), `FARMINGTON CUSTOMER SERVICE CENTER` (45,410) |
| 131 | `DUP_PO_FLG` | VARCHAR | 0 (0.0%) | distinct=3; top: `Y` (1,065,263), `N` (58,723), `U` (10,432) |
| 132 | `CRDT_MGR_CD` | VARCHAR | 0 (0.0%) | distinct=30; top: `TEAM 14` (245,386), `TEAM 96` (144,473), `TEAM 23` (130,584), `TEAM 16` (94,741), `TEAM 35` (79,111) |
| 133 | `CRDT_MGR_DSC` | VARCHAR | 0 (0.0%) | distinct=16; top: `EVELYN HICKEY` (245,386), `JANNA ANDERSON` (146,351), `CHRIS BUSBEE` (144,473), `JEFF TIEDENS` (130,584), `PRESTON DICKSON` (96,884) |
| 134 | `COLLCTN_MGR_CD` | VARCHAR | 0 (0.0%) | distinct=95; top: `WALDRUM,M` (56,602), `ALEXANDER` (46,041), `MCNEAL,D` (45,054), `ISAACS,M` (44,928), `PERKINS,C` (39,771) |
| 135 | `COLLCTN_MGR_DSC` | VARCHAR | 0 (0.0%) | distinct=81; top: `MARLENA WALDRUM` (56,602), `MARIA ALEXANDER` (46,041), `DONISHA MCNEAL` (45,054), `MARY ISAACS` (44,928), `CHERYL PERKINS` (39,771) |
| 136 | `MMS_CLASS_DSC` | VARCHAR | 0 (0.0%) | distinct=5; top: `PRIMARY CARE` (751,489), `LONG TERM CARE` (266,300), `OTHER` (116,604), `ACUTE CARE` (16), `N/A` (9) |
| 137 | `MMS_SUB_CLASS_DSC` | VARCHAR | 0 (0.0%) | distinct=39; top: `PHYSICIAN OFFICE` (501,853), `POST ACUTE CARE` (179,785), `HME` (58,760), `MISC` (51,395), `SURGERY CENTER` (36,123) |
| 138 | `DIM_CMPNY_ID` | DECIMAL(38,0) | 0 (0.0%) | min=-1, max=124, mean=22.8983, median=3 |
| 139 | `AUDIT_CRTE_ID` | DECIMAL(38,0) | 0 (0.0%) | min=0, max=1702116, mean=707593.2648, median=598927 |
| 140 | `AUDIT_CRTE_DT` | TIMESTAMP | 0 (0.0%) | min=2019-03-09 08:14:37, max=2026-04-16 00:55:20.386000 |
| 141 | `AUDIT_UPD_ID` | DECIMAL(38,0) | 0 (0.0%) | min=-10, max=1040996, mean=918889.6723, median=1010146 |
| 142 | `AUDIT_UPD_DT` | TIMESTAMP | 0 (0.0%) | min=2025-09-26 14:32:36.286000, max=2026-04-16 00:56:12.286000 |
| 143 | `PTNT_PACK_BULK_SHIP_FLG` | VARCHAR | 0 (0.0%) | distinct=3; top: `U` (1,119,309), `N` (11,963), `Y` (3,146) |
| 144 | `PRINT_CRMEMO_FLG` | VARCHAR | 0 (0.0%) | distinct=2; top: `N` (1,117,114), `Y` (17,304) |
| 145 | `EDI_TRDNG_PRTNR_ID` | VARCHAR | 0 (0.0%) | distinct=2,939; top: `N/A` (916,238), `BRIGHTREE` (14,574), `WALGREENS` (10,733), `FRESENIUS MED` (8,660), `LINCARE` (7,272) |
| 146 | `EDI_810_INV_FLG` | VARCHAR | 0 (0.0%) | distinct=2; top: `N` (967,197), `Y` (167,221) |
| 147 | `EDI_850_PO_FLG` | VARCHAR | 0 (0.0%) | distinct=2; top: `N` (1,134,256), `Y` (162) |
| 148 | `EDI_855_PO_ACK_FLG` | VARCHAR | 0 (0.0%) | distinct=2; top: `N` (968,247), `Y` (166,171) |
| 149 | `EDI_856_SHIP_NOTICE_FLG` | VARCHAR | 0 (0.0%) | distinct=2; top: `N` (974,356), `Y` (160,062) |
| 150 | `BIOMED_ACCT_FLG` | VARCHAR | 0 (0.0%) | distinct=2; top: `N` (1,108,631), `Y` (25,787) |
| 151 | `ECOMM_PURE_PLAY_ACCT_FLG` | VARCHAR | 0 (0.0%) | distinct=2; top: `N` (1,129,628), `Y` (4,790) |
| 152 | `ECOMM_MKT_PLACE_ACCT_FLG` | VARCHAR | 0 (0.0%) | distinct=2; top: `N` (1,134,231), `Y` (187) |
| 153 | `CUST_SPCLTY_ORG_CD` | VARCHAR | 0 (0.0%) | distinct=5; top: `N/A` (1,108,482), `B` (25,787), `I` (110), `G` (24), `H` (15) |
| 154 | `CUST_SPCLTY_ORG_DSC` | VARCHAR | 0 (0.0%) | distinct=5; top: `N/A` (1,108,482), `BIOMED` (25,787), `HYPERDRIVE` (110), `GOVERNMENT - BULK SHIP-TO FLY` (24), `HYPERDRIVE-ADDR OVERRIDE RIDE` (15) |
| 155 | `CUST_GLBL_LCTN_NUM` | VARCHAR | 0 (0.0%) | distinct=318,404; top: (skipped — high cardinality) |
| 156 | `CUST_CHOICE_RWRD_ELGBL_FLG` | VARCHAR | 0 (0.0%) | distinct=2; top: `Y` (673,020), `N` (461,398) |
| 157 | `CUST_CHOICE_RWRD_ENRLMNT_FLG` | VARCHAR | 0 (0.0%) | distinct=2; top: `N` (1,081,214), `Y` (53,204) |
| 158 | `DIM_CUST_PRC_GRP_LPG_ID` | DECIMAL(38,0) | 0 (0.0%) | min=-1, max=70204, mean=3920.9565, median=-1 |
| 159 | `CYP_CUST_LGCY_NUM` | VARCHAR | 0 (0.0%) | distinct=11; top: `N/A` (1,134,311), `CHH2` (34), `GEN4` (31), `GEN1050` (8), `CHH2002` (8) |
| 160 | `CNTRY_CD` | VARCHAR | 0 (0.0%) | distinct=5; top: `US` (1,134,413), `VI` (2), `BS` (1), `CA` (1), `PR` (1) |

**Sample (first 3 rows):**

|   DIM_CUST_CURR_ID |    CUST_NUM |   DIM_ACCT_MGR_CURR_ID | LGCY_PLTFRM   | SYS_PLTFRM   | BUS_PLTFRM   | CUST_NAME                  | CUST_TYPE_CD   | CUST_TYPE_DSC               |   DIM_BILL_TO_CUST_CURR_ID |   BILL_TO_CUST_NUM | ADDRSS_LINE1     | ADDRSS_LINE2         | ADDRSS_LINE3   | ADDRSS_LINE4   | CITY         | STATE   |   ZIP | PHONE_NUM    | ACTV_FLG   | DEA_LIC_EXPR_DT   | DEA_LIC_NUM   | MED_LIC_EXPR_DT     |   MED_LIC_NUM | MED_LIC_STATE_CD   | SPCLTY_DSC      | START_DT            | PTNT_FLG   | SPCLTY_CD   | SHRD_ACCT_FLG   | MKT_CD   |   MSTR_GRP_NUM | MSTR_GRP_NAME   | MMS_CLASS_CD   | MSTR_GRP_CD   |   MMS_SUB_CLASS_CD |   MMS_SGMNT_CD | MSTR_GRP_ADMIN_NAME   | MSTR_GRP_TYPE_CD   | RPT_NAMING_CD   | GOVT_CLASS_DSC   | INTRCMPNY_FLG   |   CUST_E1_NUM | CCS_DT   | CCS_CD   | CCS_DSC   |   DIM_PRMRY_GPO_ID | CCS_E1_CD   |   DIM_PRMRY_SHIP_DSTRBTN_CNTR_ID |   DIM_CUST_AR_PARNT_CURR_ID | PYMNT_TERMS_EC_CD   | PYMNT_TERMS_EC_DSC   | BID_NUM   |   CUST_AR_PARNT_NUM | HC_DLVRY_CD   | HC_DLVRY_DSC   | SITE_ADDRSS_STATE   | BKORDR_ALLOW_FLG   | GOVT_RESLR_FLG   |   CRDT_LIMIT_AMT |   AR_CALLER_CD | AR_CALLER_DSC   | AR_BUS_PLTFRM   | CNVRSN_TYPE_CD   | CNVRSN_TYPE_DSC   |   EC_BUYING_GRP_NUM | EC_BUYING_GRP_NAME   | EC_SCNDRY_BID_CD   | EC_ACCT_SPECIF_BID_CD   | EC_PROMO_BID_CD   | EC_BUY_PLAN_CD   | EC_ACCT_TYPE_CD   |   EC_SPR_GRP_NUM | EC_SPR_GRP_NAME   | GOVT_CLASS_CD   | TIER_CD   | TIER_DSC   | ECOMM_WEB_RETAIL_CD   | ECOMM_WEB_RETAIL_DSC   | CNVRSN_DT   | GOVT_COUNTRY_OF_ORIGIN_FLG   | MIN_ORDR_CHRG_EXMPT_FLG   | MIN_ORDR_CHRG_EXMPT_DT   | FUEL_SURCHRG_FLG   | T_ORDR_CD   | T_ORDR_DSC                    | ALT_ITEM_ALLOW_FLG   | PARTIAL_SHIPMNT_ALLOW_FLG   |   DIM_CUST_GRP_ID | GRP_ADMIN_NAME   |   DIM_CUST_SUPER_GRP_ID | SUPER_GRP_ADMIN_NAME   | MDM_PARTY_ID   | MDM_GUID_ID   |   DIM_PYMNT_TERMS_ID |   DIM_PYMNT_INSTRMNT_TYPE_ID |   DIM_ISS_ANALYST_ID |   DIM_CUST_AR_PROFILE_ID |   DIM_FRGHT_SCHED_ID |   LONG_ADDRSS_NUM |   ORDR_MAX_LIMIT_AMT | LAST_PYMNT_APPLD_DT   |   LAST_PYMNT_APPLD_AMT |   INV_CONSLDTN_DAY_CNT |   PHARMA_ACCT_NUM | DEFAULT_E1_EMAIL_ADDRSS   | MFG_DIRECT_CD   | FRGHT_FLG   |    PRCA_NUM | FRMLRY_CTLG_CD   | FRMLRY_CTLG_DSC   | MKTNG_PRGM_CD   | MKTNG_PRGM_DSC   | EC_HNDLNG_CD   | GOVT_GOTIT_CNTRCT_NUM   | TAX_STATUS_CD   | TAX_STATUS_DSC   | CUST_SUPPLD_ACCT_NUM   | PHARMA_XREF_NUM   | HLTH_INDSTRY_NUM   | PRMRY_AFFLN_GPO_COST_FLG   | SINGL_GSO_RSTRCTN_FLG   | PRIME_ACCT_FLG   |   SISTER_340B_ACCT_NUM | SISTER_340B_TYPE_CD   | SISTER_340B_TYPE_DSC   | RX_GPO_EXCL_TYPE_CD   | RX_GPO_EXCL_TYPE_DSC   | SRVC_CTR_CD   | SRVC_CTR_DSC                    | DUP_PO_FLG   | CRDT_MGR_CD   | CRDT_MGR_DSC   | COLLCTN_MGR_CD   | COLLCTN_MGR_DSC   | MMS_CLASS_DSC   | MMS_SUB_CLASS_DSC       |   DIM_CMPNY_ID |    AUDIT_CRTE_ID | AUDIT_CRTE_DT       |   AUDIT_UPD_ID | AUDIT_UPD_DT               | PTNT_PACK_BULK_SHIP_FLG   | PRINT_CRMEMO_FLG   | EDI_TRDNG_PRTNR_ID   | EDI_810_INV_FLG   | EDI_850_PO_FLG   | EDI_855_PO_ACK_FLG   | EDI_856_SHIP_NOTICE_FLG   | BIOMED_ACCT_FLG   | ECOMM_PURE_PLAY_ACCT_FLG   | ECOMM_MKT_PLACE_ACCT_FLG   | CUST_SPCLTY_ORG_CD   | CUST_SPCLTY_ORG_DSC   | CUST_GLBL_LCTN_NUM   | CUST_CHOICE_RWRD_ELGBL_FLG   | CUST_CHOICE_RWRD_ENRLMNT_FLG   |   DIM_CUST_PRC_GRP_LPG_ID | CYP_CUST_LGCY_NUM   | CNTRY_CD   |
|-------------------:|------------:|-----------------------:|:--------------|:-------------|:-------------|:---------------------------|:---------------|:----------------------------|---------------------------:|-------------------:|:-----------------|:---------------------|:---------------|:---------------|:-------------|:--------|------:|:-------------|:-----------|:------------------|:--------------|:--------------------|--------------:|:-------------------|:----------------|:--------------------|:-----------|:------------|:----------------|:---------|---------------:|:----------------|:---------------|:--------------|-------------------:|---------------:|:----------------------|:-------------------|:----------------|:-----------------|:----------------|--------------:|:---------|:---------|:----------|-------------------:|:------------|---------------------------------:|----------------------------:|:--------------------|:---------------------|:----------|--------------------:|:--------------|:---------------|:--------------------|:-------------------|:-----------------|-----------------:|---------------:|:----------------|:----------------|:-----------------|:------------------|--------------------:|:---------------------|:-------------------|:------------------------|:------------------|:-----------------|:------------------|-----------------:|:------------------|:----------------|:----------|:-----------|:----------------------|:-----------------------|:------------|:-----------------------------|:--------------------------|:-------------------------|:-------------------|:------------|:------------------------------|:---------------------|:----------------------------|------------------:|:-----------------|------------------------:|:-----------------------|:---------------|:--------------|---------------------:|-----------------------------:|---------------------:|-------------------------:|---------------------:|------------------:|---------------------:|:----------------------|-----------------------:|-----------------------:|------------------:|:--------------------------|:----------------|:------------|------------:|:-----------------|:------------------|:----------------|:-----------------|:---------------|:------------------------|:----------------|:-----------------|:-----------------------|:------------------|:-------------------|:---------------------------|:------------------------|:-----------------|-----------------------:|:----------------------|:-----------------------|:----------------------|:-----------------------|:--------------|:--------------------------------|:-------------|:--------------|:---------------|:-----------------|:------------------|:----------------|:------------------------|---------------:|-----------------:|:--------------------|---------------:|:---------------------------|:--------------------------|:-------------------|:---------------------|:------------------|:-----------------|:---------------------|:--------------------------|:------------------|:---------------------------|:---------------------------|:---------------------|:----------------------|:---------------------|:-----------------------------|:-------------------------------|--------------------------:|:--------------------|:-----------|
|        1.54666e+06 | 4.98001e+06 |                  37174 | MMS           | E1           | PC           | NEW LIFE SURGICAL ASC      | B              | BILL TO ADDRESS ONLY        |                1.54666e+06 |        4.98001e+06 |                  | 4253 SALISBURY RD    |                |                | JACKSONVILLE | FL      | 32216 |              | Y          | NaT               |               | NaT                 |               |                    | FAMILY PRACTICE | 2014-09-23 00:00:00 | N          | FP          | N               | PO       |            nan | N/A             | B              | N/A           |                 14 |             14 | N/A                   | N/A                | N/A             | NOT USED         | N               |   4.98001e+06 | NaT      |          |           |   260139           |             |                             3044 |                          -1 | N/A                 | N/A                  | N/A       |                   0 | N/A           | BLANK          | N/A                 | Y                  | N                |            25009 |            963 | ALBALOS,K       | PC              | N/A              | N/A               |                 nan | N/A                  | N/A                | N/A                     | N/A               | N/A              | N/A               |              nan | N/A               | N/A             | N/A       | N/A        | N/A                   | N/A                    | NaT         | U                            | N                         | 2014-12-23 00:00:00      | Y                  | F           | ALT SOURCG ALLOWED FRT EXEMPT | N                    | Y                           |                -1 | N/A              |                      -1 | N/A                    |                |               |                  372 |                           28 |                  199 |                    35627 |                 1374 |          01101626 |                25009 | 2026-04-08 00:00:00   |                -920.77 |                      0 |                 0 | N/A                       | N/A             | Y           | 4.98001e+06 | N/A              | N/A               | N/A             | N/A              | N/A            |                         | T               | TAXABLE          | N/A                    | N/A               | N/A                | N                          | N                       | N                |                      0 | NA                    | UNKNOWN                | N/A                   | N/A                    | ATL           | ATLANTA CUSTOMER SERVICE CENTER | Y            | TEAM 96       | CHRIS BUSBEE   | ALBALOS,K        | KEANA ALBALOS     | PRIMARY CARE    | PHYSICIAN OFFICE        |              3 | 598927           | 2019-03-09 08:53:03 |    1.03723e+06 | 2026-04-10 00:51:14.438000 | U                         | N                  | N/A                  | N                 | N                | N                    | N                         | N                 | N                          | N                          | N/A                  | N/A                   | 1100007612288        | Y                            | Y                              |                        -1 | N/A                 | US         |
|        1.52002e+07 | 7.25001e+07 |                  39861 | MMS           | E1           | PC           | J HALL ENTERPRISE LLC      | B              | BILL TO ADDRESS ONLY        |                1.52002e+07 |        7.25001e+07 | CURV BODY LOUNGE | 4207 N SCOTTSDALE RD |                |                | SCOTTSDALE   | AZ      | 85251 | 480-571-4767 | Y          | NaT               |               | NaT                 |               |                    | FAMILY PRACTICE | 2021-03-18 00:00:00 | N          | FP          | N               | PO       |            nan | N/A             | B              | N/A           |                 12 |             14 | N/A                   | N/A                | N/A             | NOT USED         | N               |   7.25001e+07 | NaT      |          |           |       -1           |             |                             4978 |                          -1 | N/A                 | N/A                  | N/A       |                   0 | N/A           | BLANK          | N/A                 | Y                  | N                |                1 |            237 | WILLS,S         | PC              | N/A              | N/A               |                 nan | N/A                  | N/A                | N/A                     | N/A               | N/A              | N/A               |              nan | N/A               | N/A             | N/A       | N/A        | N/A                   | N/A                    | NaT         | U                            | N                         | NaT                      | Y                  | F           | ALT SOURCG ALLOWED FRT EXEMPT | N                    | Y                           |                -1 | N/A              |                      -1 | N/A                    |                |               |                   66 |                           28 |                  199 |                    91513 |                 1374 |          08207056 |                10000 | 2026-04-08 00:00:00   |               -1233.81 |                      0 |                 0 | N/A                       | N/A             | Y           | 7.25001e+07 | N/A              | N/A               | N/A             | N/A              | N/A            |                         | T               | TAXABLE          | N/A                    | N/A               | N/A                | N                          | N                       | N                |                      0 | NA                    | UNKNOWN                | N/A                   | N/A                    | N/A           | N/A                             | Y            | TEAM 23       | JEFF TIEDENS   | WILLS,S          | SHEILA WILLS      | PRIMARY CARE    | OCCUPATIONAL HEALTH     |              3 |      1.36808e+06 | 2021-03-19 06:20:50 |    1.03723e+06 | 2026-04-10 00:51:14.438000 | U                         | N                  | N/A                  | N                 | N                | N                    | N                         | N                 | N                          | N                          | N/A                  | N/A                   | N/A                  | Y                            | N                              |                        -1 | N/A                 | US         |
|        1.59762e+06 | 5.00568e+06 |                  60115 | MMS           | E1           | PC           | UNIVERSITY EYE SPECIALISTS | X              | BILL TO AND SHIP TO ADDRESS |                1.59762e+06 |        5.00568e+06 |                  | 2469 STATE ROUTE 19  |                |                | WARSAW       | NY      | 14569 | 585-786-2288 | Y          | NaT               |               | 2027-03-31 00:00:00 |        162161 | NY                 | OPHTHALMOLOGY   | 1995-09-02 00:00:00 | N          | OPH         | N               | PO       |            nan | N/A             | B              | N/A           |                 21 |             14 | N/A                   | N/A                | N/A             | COMMERCIAL       | N               |   5.00568e+06 | NaT      |          |           |        3.50753e+06 | P           |                             2496 |                          -1 | N/A                 | N/A                  | N/A       |                   0 | N/A           | BLANK          | N/A                 | Y                  | N                |             9709 |            965 | BODIN,B         | PC              | N/A              | N/A               |                 nan | N/A                  | N/A                | N/A                     | N/A               | N/A              | N/A               |              nan | N/A               | X               | N/A       | N/A        | N/A                   | N/A                    | NaT         | U                            | Y                         | 2099-12-31 00:00:00      | Y                  | F           | ALT SOURCG ALLOWED FRT EXEMPT | Y                    | Y                           |                -1 | N/A              |                      -1 | N/A                    |                |               |                  136 |                           28 |                  199 |                    71069 |                 1383 |          26300263 |                10000 | 2026-04-08 00:00:00   |              -12772.1  |                      0 |                 0 | N/A                       | N/A             | Y           | 5.00568e+06 | N/A              | N/A               | N/A             | N/A              | N/A            |                         | T               | TAXABLE          | N/A                    | 1003237187        | 85YN4FH00          | Y                          | N                       | N                |                      0 | NA                    | UNKNOWN                | N/A                   | N/A                    | N/A           | N/A                             | Y            | TEAM 96       | CHRIS BUSBEE   | BODIN,B          | BLAKE BODIN       | PRIMARY CARE    | GOVERNMENT-PRIMARY CARE |              3 | 598927           | 2019-03-09 08:50:52 |    1.03723e+06 | 2026-04-10 00:51:14.438000 | U                         | N                  | N/A                  | N                 | N                | N                    | N                         | N                 | N                          | N                          | N/A                  | N/A                   | 1100006055819        | Y                            | N                              |                        -1 | N/A                 | US         |

### `v_dim_item_curr_revised` _(partitioned dataset, 16 shards)_

- **Folder:** `C:\Users\maina\Desktop\Capstone\data_raw\v_dim_item_curr_revised`
- **Total size:** 34.31 MB
- **Rows:** 277,203
- **Columns:** 125

**Columns:**

| # | Column | Type | Nulls | Stats |
|---|---|---|---|---|
| 1 | `DIM_ITEM_E1_CURR_ID` | DECIMAL(38,0) | 0 (0.0%) | min=-1, max=44477195, mean=9306595.848, median=807150 |
| 2 | `ITEM_E1_NUM` | DECIMAL(38,0) | 0 (0.0%) | min=-1, max=1291533, mean=955251.2351, median=1052457 |
| 3 | `ITEM_DSC` | VARCHAR | 0 (0.0%) | distinct=270,541; top: (skipped — high cardinality) |
| 4 | `DIM_SUPLR_CURR_ID` | DECIMAL(38,0) | 0 (0.0%) | min=-11, max=496351, mean=109341.0056, median=97658 |
| 5 | `DIM_SUPLR_ABBRV_ID` | DECIMAL(38,0) | 0 (0.0%) | min=-1, max=15357, mean=4854.882, median=3819 |
| 6 | `PARNT_SUPLR_NUM` | DECIMAL(38,0) | 0 (0.0%) | min=0, max=98832329, mean=9840015.6367, median=3884619 |
| 7 | `PARNT_SUPLR_NAME` | VARCHAR | 0 (0.0%) | distinct=1,702; top: `MCKESSON PHARMACEUTICALS` (8,075), `INTEGRA YORK PA INC` (8,004), `STERIS CORPORATION` (7,024), `MIDMARK CORPORATION` (5,473), `THERMO SCIENTIFIC (FISHER)` (4,782) |
| 8 | `PARNT_SUPLR_SRCH_TYPE` | VARCHAR | 0 (0.0%) | distinct=2; top: `VB` (277,165), `N/A` (38) |
| 9 | `SUPLR_ABBRV` | VARCHAR | 0 (0.0%) | distinct=2,759; top: `CARVMU` (6,023), `MILTEX` (5,127), `MIDMRK` (4,706), `UNDSTR` (4,504), `SKLAR` (4,165) |
| 10 | `SUPLR_DSC` | VARCHAR | 0 (0.0%) | distinct=2,432; top: `STERIS CORP (CAREFUSION)` (6,023), `MILTEX INSTRUMENT (INTEGRA YK)` (5,127), `MIDMARK CORP` (4,706), `ESSENDANT CO/UNITED STATIONERS` (4,504), `SKLAR INSTRUMENT CORP` (4,174) |
| 11 | `SUPLR_ROLLUP_DSC` | VARCHAR | 0 (0.0%) | distinct=2,207; top: `STERIS CORPORATION` (8,121), `MIDMARK CORP` (5,473), `MILTEX INSTRUMENT (INTEGRA YK)` (5,436), `THERMO SCIENTIFIC (FISHER)` (4,782), `CARDINAL HEALTH` (4,708) |
| 12 | `SELL_CORP_ACQ_COST_AMT` | DECIMAL(38,10) | 48 (0.0%) | min=0E-10, max=1621735.0000000000, mean=1156.1083, median=91.0000000000 |
| 13 | `CTLG_NUM` | VARCHAR | 1 (0.0%) | distinct=262,058; top: (skipped — high cardinality) |
| 14 | `BUY_UOM` | VARCHAR | 0 (0.0%) | distinct=42; top: `EA` (153,446), `CS` (61,108), `BX` (30,494), `PK` (11,691), `CT` (3,687) |
| 15 | `SELL_UOM` | VARCHAR | 0 (0.0%) | distinct=46; top: `EA` (157,963), `CS` (40,345), `BX` (38,895), `PK` (14,669), `CT` (3,757) |
| 16 | `PRMRY_UOM` | VARCHAR | 0 (0.0%) | distinct=46; top: `EA` (245,038), `BX` (6,613), `PK` (5,277), `PR` (4,646), `BT` (3,356) |
| 17 | `ATOMIC_UOM` | VARCHAR | 0 (0.0%) | distinct=13; top: `UN` (136,543), `N/A` (126,748), `OZ` (4,360), `ML` (3,501), `FT` (3,036) |
| 18 | `ATOMIC_TO_PRMRY_DIVISOR` | DECIMAL(38,10) | 126,747 (45.7%) | min=0E-10, max=96000.0000000000, mean=26.9709, median=1.0000000000 |
| 19 | `SELL_TO_BUY_DIVISOR` | DECIMAL(38,10) | 0 (0.0%) | min=0E-10, max=5000.0000000000, mean=2.4238, median=1.0000000000 |
| 20 | `PRMRY_TO_SELL_DIVISOR` | DECIMAL(38,10) | 0 (0.0%) | min=0E-10, max=25000.0000000000, mean=35.6473, median=1.0000000000 |
| 21 | `PRMRY_TO_BUY_DIVISOR` | DECIMAL(38,10) | 0 (0.0%) | min=0E-10, max=182000.0000000000, mean=80.3534, median=1.0000000000 |
| 22 | `PRMRY_TO_ATOMIC_DIVISOR` | DECIMAL(38,10) | 126,747 (45.7%) | min=0E-10, max=200.0000000000, mean=0.889, median=1.0000000000 |
| 23 | `PROD_TYPE_CD` | VARCHAR | 0 (0.0%) | distinct=13; top: `N/A` (230,267), `E` (23,000), `K` (16,667), `L` (2,420), `F` (2,049) |
| 24 | `PROD_TYPE_DSC` | VARCHAR | 230,266 (83.1%) | distinct=13; top: `EQUIPMENT` (23,000), `CUSTOM` (16,667), `LOT CONTROL` (2,420), `EQUIPMENT - LABORATORY` (2,049), `INJECTABLE` (1,813) |
| 25 | `PROD_GRP_CD` | VARCHAR | 0 (0.0%) | distinct=6; top: `MS` (214,828), `LB` (27,575), `EQ` (22,989), `RX` (10,678), `OT` (1,132) |
| 26 | `PROD_GRP_DSC` | VARCHAR | 0 (0.0%) | distinct=6; top: `MEDSURG` (214,828), `LAB` (27,575), `EQUIPMENT` (22,989), `RX` (10,678), `NON-PRODUCT` (1,132) |
| 27 | `PROD_SUB_GRP_CD` | VARCHAR | 0 (0.0%) | distinct=20; top: `MS` (214,828), `EQ` (22,989), `RN` (12,589), `SU` (10,517), `BR` (6,698) |
| 28 | `PROD_SUB_GRP_DSC` | VARCHAR | 0 (0.0%) | distinct=20; top: `MEDSURG` (214,828), `EQUIPMENT` (22,989), `REAGENTS NW` (12,589), `LAB SUPPLIES` (10,517), `BRAND RX` (6,698) |
| 29 | `CTGRY_CD` | VARCHAR | 0 (0.0%) | distinct=28; top: `N/A` (91,067), `C` (29,968), `U` (26,608), `S` (16,026), `L` (15,121) |
| 30 | `CTGRY_DSC` | VARCHAR | 0 (0.0%) | distinct=28; top: `N/A` (91,067), `INSTRUMENTS & SURGICAL` (29,968), `LABORATORY` (26,608), `ORTHOPEDICS` (16,026), `DME,  PHYSICAL THERAPY & ADL` (15,121) |
| 31 | `SUB_CTGRY_CD` | VARCHAR | 0 (0.0%) | distinct=812; top: `N/A` (91,067), `CA` (15,182), `UEB` (5,894), `PAA` (4,075), `AA` (3,590) |
| 32 | `SUB_CTGRY_DSC` | VARCHAR | 0 (0.0%) | distinct=785; top: `N/A` (91,067), `GENERAL INSTRUMENTS` (15,182), `LARGE CHEMISTRY REAGENTS` (5,894), `UNITED STATIONER OFFC SUP PROG` (4,075), `SUTURES` (3,590) |
| 33 | `DISCTND_FLG` | VARCHAR | 0 (0.0%) | distinct=1; top: `N` (277,203) |
| 34 | `DISCTND_DT` | TIMESTAMP | 0 (0.0%) | min=1900-01-01 00:00:00, max=9999-12-31 00:00:00 |
| 35 | `ITEM_STATUS_CD` | VARCHAR | 0 (0.0%) | distinct=10; top: `N/A` (268,613), `WSA` (7,384), `CON` (687), `RSK` (487), `DNA` (17) |
| 36 | `ITEM_STATUS_DSC` | VARCHAR | 0 (0.0%) | distinct=10; top: `N/A` (268,613), `MCKESSON PHARMACEUTICALS` (7,384), `CONTROLLED SUB (CONS99)` (687), `SKU RESTRICTED` (487), `DISCONTINUED NO ACTIVITY` (17) |
| 37 | `PRVT_BRND_FLG` | VARCHAR | 0 (0.0%) | distinct=2; top: `N` (272,848), `Y` (4,355) |
| 38 | `NDC_NUM` | VARCHAR | 264,252 (95.3%) | distinct=12,441; top: `55141019298` (28), `00268024601` (8), `11111000101` (7), `70461065503` (7), `22840001904` (6) |
| 39 | `STNDRD_COST_AMT` | DECIMAL(38,10) | 42 (0.0%) | min=0E-10, max=1621735.0000000000, mean=1154.5916, median=90.6500000000 |
| 40 | `MAC_COST_AMT` | DECIMAL(38,10) | 272,401 (98.3%) | min=0E-10, max=7748.7200000000, mean=63.0368, median=11.5633500000 |
| 41 | `FED_LEGEND_CD` | VARCHAR | 0 (0.0%) | distinct=4; top: `N` (192,632), `M` (76,340), `Y` (8,229), `N/A` (2) |
| 42 | `LEGEND_CD` | VARCHAR | 0 (0.0%) | distinct=109; top: `N` (192,632), `MD1` (23,235), `MD8` (8,757), `HH1` (6,226), `DD1` (5,257) |
| 43 | `GNRC_FLG` | VARCHAR | 0 (0.0%) | distinct=2; top: `N` (271,128), `Y` (6,075) |
| 44 | `PHARMA_ITEM_NUM` | VARCHAR | 269,239 (97.1%) | distinct=7,600; top: `3704897` (8), `3014958` (7), `2687192` (4), `3010782` (4), `3010790` (4) |
| 45 | `GCN_NUM` | VARCHAR | 269,378 (97.2%) | distinct=3,841; top: `2962` (51), `6641` (32), `94200` (24), `14021` (20), `35930` (18) |
| 46 | `GCN_DOSE_FRM` | VARCHAR | 0 (0.0%) | distinct=186; top: `N/A` (269,400), `TABLET` (2,234), `VIAL` (1,475), `CAPSULE` (490), `SYRINGE` (302) |
| 47 | `GHX_MAJOR_CD` | VARCHAR | 0 (0.0%) | distinct=1; top: `N/A` (277,203) |
| 48 | `GHX_MAJOR_DSC` | VARCHAR | 0 (0.0%) | distinct=1; top: `N/A` (277,203) |
| 49 | `GHX_MINOR_CD` | VARCHAR | 0 (0.0%) | distinct=1; top: `N/A` (277,203) |
| 50 | `GHX_MINOR_DSC` | VARCHAR | 0 (0.0%) | distinct=1; top: `N/A` (277,203) |
| 51 | `EXCESS_ITEM_CD` | VARCHAR | 0 (0.0%) | distinct=4; top: `-` (272,697), `O` (4,504), `B` (1), `*` (1) |
| 52 | `NRCTC_CD` | VARCHAR | 0 (0.0%) | distinct=9; top: `N` (274,510), `N/A` (2,001), `2` (240), `4` (216), `L` (109) |
| 53 | `STRG_CD` | VARCHAR | 45 (0.0%) | distinct=6; top: `N` (254,017), `Y` (11,330), `R` (9,484), `F` (2,299), `HAZ` (27) |
| 54 | `TAX_CD` | VARCHAR | 205 (0.1%) | distinct=352; top: `97127` (21,603), `97096` (19,500), `97095` (18,272), `97238` (17,164), `97192` (12,992) |
| 55 | `FED_HZRD_CD` | VARCHAR | 0 (0.0%) | distinct=76; top: `N/A` (272,687), `LTDQTY` (3,760), `NO AIR` (241), `UN1170` (85), `UN1230` (64) |
| 56 | `STOCK_TYPE_CD` | VARCHAR | 0 (0.0%) | distinct=4; top: `P` (276,165), `I` (1,029), `9` (8), `*` (1) |
| 57 | `SLS_GRP_CD` | VARCHAR | 0 (0.0%) | distinct=2,198; top: `2304` (5,583), `4443` (5,474), `4484` (5,151), `5054` (4,691), `1311` (4,506) |
| 58 | `SLS_GRP_DSC` | VARCHAR | 0 (0.0%) | distinct=2,156; top: `CAREFUSION 213 LLC` (5,583), `MIDMARK` (5,474), `MILTEX INSTRUMENT COMPANY INCO` (5,151), `CARDINAL HEALTH` (4,691), `UNITED STATIONERS SUPPLY CO` (4,506) |
| 59 | `INBND_PEDIGREE_CD` | VARCHAR | 0 (0.0%) | distinct=5; top: `N/A` (268,974), `52` (4,924), `74` (1,569), `22` (1,466), `29` (270) |
| 60 | `GL_CLASS_CD` | VARCHAR | 1 (0.0%) | distinct=14; top: `IN30` (276,292), `IN57` (462), `IN56` (366), `FH10` (19), `SF40` (17) |
| 61 | `ITEM_TYPE_CD` | VARCHAR | 0 (0.0%) | distinct=5; top: `N/A` (219,286), `DS` (57,390), `PDS` (377), `BDS` (148), `CON` (2) |
| 62 | `OTC_FLG` | VARCHAR | 0 (0.0%) | distinct=2; top: `N` (274,894), `Y` (2,309) |
| 63 | `SORT_SEQ_NUM` | DECIMAL(38,0) | 158 (0.1%) | min=0, max=52, mean=48.9945, median=52 |
| 64 | `HPIS_SKU` | VARCHAR | 1 (0.0%) | distinct=1; top: `N/A` (277,202) |
| 65 | `GHX_MATCH_LEVEL_CD` | VARCHAR | 1 (0.0%) | distinct=1; top: `N/A` (277,202) |
| 66 | `COMP_CTGRY_CD` | VARCHAR | 906 (0.3%) | distinct=6; top: `Y` (166,973), `B` (48,369), `R` (40,899), `P` (10,296), `BX` (5,322) |
| 67 | `COMP_CTGRY_DSC` | VARCHAR | 906 (0.3%) | distinct=6; top: `Yellow` (166,973), `Blue` (48,369), `Red` (40,899), `Platinum` (10,296), `Blue Rx` (5,322) |
| 68 | `MFG_STATUS_CD` | VARCHAR | 0 (0.0%) | distinct=2; top: `N/A` (263,996), `A` (13,207) |
| 69 | `MFG_STATUS_DSC` | VARCHAR | 0 (0.0%) | distinct=2; top: `N/A` (263,996), `ALIGNED` (13,207) |
| 70 | `PROD_CLASS_CD` | VARCHAR | 1 (0.0%) | distinct=1,312; top: `N/ NN` (66,475), `CA NN` (12,829), `N/ NE` (11,608), `N/ NK` (10,460), `PA NN` (4,822) |
| 71 | `PROD_FMLY_LVL1_CD` | VARCHAR | 0 (0.0%) | distinct=18; top: `F01` (80,649), `F05` (68,490), `F12` (21,688), `F10` (20,213), `F11` (15,432) |
| 72 | `PROD_FMLY_LVL1_DSC` | VARCHAR | 0 (0.0%) | distinct=18; top: `Nursing and Surgical Supplies` (80,649), `Equipment & Equip Disposables` (68,490), `Lab-Ancillary Lab Products` (21,688), `Lab-Non-Waived Lab` (20,213), `Office and Facility Supplies` (15,432) |
| 73 | `PROD_CTGRY_LVL2_CD` | VARCHAR | 0 (0.0%) | distinct=107; top: `C072` (27,255), `C055` (18,684), `C027` (17,010), `C044` (11,941), `C008` (10,808) |
| 74 | `PROD_CTGRY_LVL2_DSC` | VARCHAR | 0 (0.0%) | distinct=107; top: `Surgical Instruments` (27,255), `Orthopedics` (18,684), `Exam & Patient Room Furnishing` (17,010), `Lab-Lab Supplies` (11,941), `Beds and Patient Safety` (10,808) |
| 75 | `PROD_GRP_LVL3_CD` | VARCHAR | 0 (0.0%) | distinct=531; top: `G0124` (19,583), `G0164` (9,396), `G0068` (7,724), `G0654` (7,012), `G0100` (6,288) |
| 76 | `PROD_GRP_LVL3_DSC` | VARCHAR | 0 (0.0%) | distinct=529; top: `Non-Sterile Surg Instruments` (19,583), `Glassware, Plasticware & Consu` (9,396), `Chemistry Reagents` (7,724), `Soft Goods - Lower Extrem` (7,012), `Custom` (6,288) |
| 77 | `PROD_SUB_CTGRY_LVL4_CD` | VARCHAR | 0 (0.0%) | distinct=2,425; top: `S00634` (5,227), `S00379` (4,367), `S01727` (2,966), `S02191` (2,849), `S01397` (2,705) |
| 78 | `PROD_SUB_CTGRY_LVL4_DSC` | VARCHAR | 0 (0.0%) | distinct=2,410; top: `Exam Tables/Procedure Chairs` (5,227), `Chemistry Reagents` (4,367), `Scrubs, Reusable` (2,966), `Labels/Signs` (2,849), `Other Lab Consumables` (2,705) |
| 79 | `PROD_CTGRY_PC_FIN_ROLLUP_CD` | VARCHAR | 0 (0.0%) | distinct=21; top: `MS` (148,769), `EQ` (68,490), `SU` (20,200), `RN` (19,195), `RX` (11,162) |
| 80 | `PROD_CTGRY_PC_FIN_ROLLUP_DSC` | VARCHAR | 0 (0.0%) | distinct=20; top: `MEDSURG` (148,764), `EQUIPMENT` (68,490), `LAB SUPPLIES` (20,259), `REAGENTS NW` (19,159), `RX` (11,162) |
| 81 | `PROD_CTGRY_EC_FIN_ROLLUP_DSC` | VARCHAR | 0 (0.0%) | distinct=18; top: `OTHER MEDSURG` (125,583), `EQUIPMENT` (68,490), `LAB` (45,002), `RX` (11,162), `RESPIRATORY` (8,073) |
| 82 | `PROD_CTGRY_LAB_FIN_ROLLUP_DSC` | VARCHAR | 0 (0.0%) | distinct=15; top: `LAB SUPPLIES` (258,325), `REAGENTS - NON-SIEMENS CHEM/IA` (8,908), `REAGENTS - OTHER NW (BLOOD BANK, COAG, ETC)` (3,019), `REAGENTS - SIEMENS` (2,875), `REAGENTS - MICRO` (2,239) |
| 83 | `MFG_BKORDR_REL_DT` | TIMESTAMP | 263,270 (95.0%) | min=1900-01-01 00:00:00, max=2099-12-31 00:00:00 |
| 84 | `MFG_BKORDR_DUE_DT` | TIMESTAMP | 263,270 (95.0%) | min=1900-01-01 00:00:00, max=2100-01-14 00:00:00 |
| 85 | `MFG_BKORDR_DSC` | VARCHAR | 0 (0.0%) | distinct=153; top: `N/A` (263,271), `No Release Date ` (8,072), `Overstock ` (826), `Estimated Release Date 04/30/2026` (758), `On Allocation ` (674) |
| 86 | `MFG_BKORDR_FLG` | VARCHAR | 0 (0.0%) | distinct=2; top: `N` (263,271), `Y` (13,932) |
| 87 | `NETWRK_VELCTY_CD` | VARCHAR | 0 (0.0%) | distinct=9; top: `Z` (224,211), `C` (31,403), `D` (9,093), `B` (4,913), `E` (3,250) |
| 88 | `NETWRK_VELCTY_DSC` | VARCHAR | 0 (0.0%) | distinct=9; top: `NTWK NSTK-NO SLS-NO INV` (224,211), `STOCK ITEM` (31,403), `NTWK NSTK-SOME SLS` (9,093), `TOP 90% LINE VOLUME` (4,913), `NTWK NSTK-NO SLS-SOME INV` (3,250) |
| 89 | `ITEM_RISK_SCORE_CD` | VARCHAR | 0 (0.0%) | distinct=10; top: `4` (241,268), `1` (10,603), `3` (9,395), `2` (7,997), `4R` (5,546) |
| 90 | `ITEM_RISK_SCORE_DSC` | VARCHAR | 0 (0.0%) | distinct=9; top: `MID - HIGH RISK` (241,268), `LOW RISK` (10,603), `MID RISK` (9,395), `MID-LOW RISK` (7,997), `MID - HIGH RISK - 12MNM` (5,546) |
| 91 | `CORP_LOW_UOM` | VARCHAR | 0 (0.0%) | distinct=44; top: `EA` (171,747), `BX` (34,114), `CS` (32,955), `PK` (14,226), `PR` (3,700) |
| 92 | `PRMRY_UOM_WGHT` | DECIMAL(38,10) | 90,142 (32.5%) | min=0E-10, max=9999.0000000000, mean=13.9116, median=0.3000000000 |
| 93 | `STRG_TYPE_CD` | VARCHAR | 0 (0.0%) | distinct=3; top: `N/A` (275,352), `LF` (1,813), `LFM` (38) |
| 94 | `STRG_TYPE_DSC` | VARCHAR | 0 (0.0%) | distinct=3; top: `N/A` (275,352), `PICK BY LOT FEFO` (1,813), `LOT FEFO MCKESSON CUSTOM LOGIC` (38) |
| 95 | `OBSLT_DT` | TIMESTAMP | 0 (0.0%) | min=1900-01-01 00:00:00, max=9999-12-31 00:00:00 |
| 96 | `FLU_PREBOOK_FLG` | VARCHAR | 0 (0.0%) | distinct=1; top: `N` (277,203) |
| 97 | `DEAD_NET_COST_AMT` | DECIMAL(38,10) | 274,406 (99.0%) | min=0.4485000000, max=3319.8660000000, mean=28.0285, median=9.4013000000 |
| 98 | `EC_VIP_COST_AMT` | DECIMAL(38,10) | 272,426 (98.3%) | min=0E-10, max=8341.7500000000, mean=157.2136, median=61.5200000000 |
| 99 | `ALT_CTLG_NUM` | VARCHAR | 264,229 (95.3%) | distinct=12,410; top: `Y` (16), `847622` (7), `1246036` (6), `829973` (4), `1263649` (4) |
| 100 | `SPCLTY_RX_FLG` | VARCHAR | 0 (0.0%) | distinct=3; top: `N` (276,850), `Y` (352), `U` (1) |
| 101 | `UNSPSC_CD` | VARCHAR | 108,082 (39.0%) | distinct=4,784; top: `42312201` (3,243), `42291802` (3,042), `41116004` (1,850), `41116107` (1,826), `42241706` (1,774) |
| 102 | `UNSPSC_DSC` | VARCHAR | 108,082 (39.0%) | distinct=4,790; top: `Sutures` (3,243), `Surgical clamps or clips or forceps` (3,042), `Chemistry analyzer reagents` (1,850), `Chemistry quality controls or calibrators or standards` (1,826), `Orthotics or foot care products` (1,774) |
| 103 | `UNSPSC_VRSN` | VARCHAR | 108,082 (39.0%) | distinct=2; top: `UNSPSC v26.0801` (169,120), `N/A` (1) |
| 104 | `DSCSA_ITEM_FLG` | VARCHAR | 0 (0.0%) | distinct=2; top: `N` (270,443), `Y` (6,760) |
| 105 | `TAA_FLG` | VARCHAR | 0 (0.0%) | distinct=2; top: `Y` (143,451), `N` (133,752) |
| 106 | `BAA_FLG` | VARCHAR | 0 (0.0%) | distinct=2; top: `N` (196,964), `Y` (80,239) |
| 107 | `PPE_FLG` | VARCHAR | 0 (0.0%) | distinct=2; top: `N` (270,209), `Y` (6,994) |
| 108 | `LGCY_PLTFRM` | VARCHAR | 0 (0.0%) | distinct=1; top: `MMS` (277,203) |
| 109 | `CYP_BRND_CD` | VARCHAR | 0 (0.0%) | distinct=1; top: `N/A` (277,203) |
| 110 | `MIN_BATCH_QTY` | DECIMAL(8,0) | 0 (0.0%) | min=0, max=0, mean=0.0, median=0 |
| 111 | `HIGH_ROWS_PER_PALLET` | DECIMAL(3,0) | 0 (0.0%) | min=0, max=0, mean=0.0, median=0 |
| 112 | `BATCH_QTY_MULTIPLE` | DECIMAL(8,0) | 0 (0.0%) | min=0, max=0, mean=0.0, median=0 |
| 113 | `TARIFF_CD` | VARCHAR | 0 (0.0%) | distinct=1; top: `N/A` (277,203) |
| 114 | `TIE_CASES_PER_ROW` | DECIMAL(3,0) | 0 (0.0%) | min=0, max=0, mean=0.0, median=0 |
| 115 | `LCT_REQD_FLG` | VARCHAR | 0 (0.0%) | distinct=1; top: `N` (277,203) |
| 116 | `BY_TRANS_MODE` | VARCHAR | 0 (0.0%) | distinct=1; top: `N/A` (277,203) |
| 117 | `GS_MGR_ID` | DECIMAL(10,0) | 0 (0.0%) | min=0, max=0, mean=0.0, median=0 |
| 118 | `TRACEABLE_TYPE` | VARCHAR | 0 (0.0%) | distinct=1; top: `N/A` (277,203) |
| 119 | `PARNT_ITEM_NUM` | DECIMAL(10,0) | 0 (0.0%) | min=0, max=1290474, mean=8307.3723, median=0 |
| 120 | `BY_UOM_CONV_TO_PARNT` | DECIMAL(11,8) | 0 (0.0%) | min=0E-8, max=1.00000000, mean=0.0065, median=0E-8 |
| 121 | `OVERPACK_FLG` | VARCHAR | 0 (0.0%) | distinct=1; top: `N` (277,203) |
| 122 | `AUDIT_CRTE_ID` | DECIMAL(38,0) | 0 (0.0%) | min=0, max=1701989, mean=332942.6974, median=93508 |
| 123 | `AUDIT_CRTE_DT` | TIMESTAMP | 0 (0.0%) | min=2016-10-14 18:17:26, max=2026-04-16 00:55:51.652000 |
| 124 | `AUDIT_UPD_ID` | DECIMAL(38,0) | 0 (0.0%) | min=0, max=1040997, mean=1011895.7726, median=1023637 |
| 125 | `AUDIT_UPD_DT` | TIMESTAMP | 0 (0.0%) | min=2025-09-19 14:16:07.294000, max=2026-04-16 00:55:56.843000 |

**Sample (first 3 rows):**

|   DIM_ITEM_E1_CURR_ID |   ITEM_E1_NUM | ITEM_DSC                                                   |   DIM_SUPLR_CURR_ID |   DIM_SUPLR_ABBRV_ID |   PARNT_SUPLR_NUM | PARNT_SUPLR_NAME             | PARNT_SUPLR_SRCH_TYPE   | SUPLR_ABBRV   | SUPLR_DSC                    | SUPLR_ROLLUP_DSC   |   SELL_CORP_ACQ_COST_AMT | CTLG_NUM   | BUY_UOM   | SELL_UOM   | PRMRY_UOM   | ATOMIC_UOM   |   ATOMIC_TO_PRMRY_DIVISOR |   SELL_TO_BUY_DIVISOR |   PRMRY_TO_SELL_DIVISOR |   PRMRY_TO_BUY_DIVISOR |   PRMRY_TO_ATOMIC_DIVISOR | PROD_TYPE_CD   | PROD_TYPE_DSC   | PROD_GRP_CD   | PROD_GRP_DSC   | PROD_SUB_GRP_CD   | PROD_SUB_GRP_DSC   | CTGRY_CD   | CTGRY_DSC   | SUB_CTGRY_CD   | SUB_CTGRY_DSC   | DISCTND_FLG   | DISCTND_DT          | ITEM_STATUS_CD   | ITEM_STATUS_DSC   | PRVT_BRND_FLG   | NDC_NUM   |   STNDRD_COST_AMT |   MAC_COST_AMT | FED_LEGEND_CD   | LEGEND_CD   | GNRC_FLG   | PHARMA_ITEM_NUM   | GCN_NUM   | GCN_DOSE_FRM   | GHX_MAJOR_CD   | GHX_MAJOR_DSC   | GHX_MINOR_CD   | GHX_MINOR_DSC   | EXCESS_ITEM_CD   | NRCTC_CD   | STRG_CD   |   TAX_CD | FED_HZRD_CD   | STOCK_TYPE_CD   |   SLS_GRP_CD | SLS_GRP_DSC           | INBND_PEDIGREE_CD   | GL_CLASS_CD   | ITEM_TYPE_CD   | OTC_FLG   |   SORT_SEQ_NUM | HPIS_SKU   | GHX_MATCH_LEVEL_CD   | COMP_CTGRY_CD   | COMP_CTGRY_DSC   | MFG_STATUS_CD   | MFG_STATUS_DSC   | PROD_CLASS_CD   | PROD_FMLY_LVL1_CD   | PROD_FMLY_LVL1_DSC            | PROD_CTGRY_LVL2_CD   | PROD_CTGRY_LVL2_DSC            | PROD_GRP_LVL3_CD   | PROD_GRP_LVL3_DSC      | PROD_SUB_CTGRY_LVL4_CD   | PROD_SUB_CTGRY_LVL4_DSC    | PROD_CTGRY_PC_FIN_ROLLUP_CD   | PROD_CTGRY_PC_FIN_ROLLUP_DSC   | PROD_CTGRY_EC_FIN_ROLLUP_DSC   | PROD_CTGRY_LAB_FIN_ROLLUP_DSC   | MFG_BKORDR_REL_DT   | MFG_BKORDR_DUE_DT   | MFG_BKORDR_DSC   | MFG_BKORDR_FLG   | NETWRK_VELCTY_CD   | NETWRK_VELCTY_DSC       |   ITEM_RISK_SCORE_CD | ITEM_RISK_SCORE_DSC   | CORP_LOW_UOM   |   PRMRY_UOM_WGHT | STRG_TYPE_CD   | STRG_TYPE_DSC   | OBSLT_DT            | FLU_PREBOOK_FLG   |   DEAD_NET_COST_AMT |   EC_VIP_COST_AMT | ALT_CTLG_NUM   | SPCLTY_RX_FLG   | UNSPSC_CD   | UNSPSC_DSC   | UNSPSC_VRSN   | DSCSA_ITEM_FLG   | TAA_FLG   | BAA_FLG   | PPE_FLG   | LGCY_PLTFRM   | CYP_BRND_CD   |   MIN_BATCH_QTY |   HIGH_ROWS_PER_PALLET |   BATCH_QTY_MULTIPLE | TARIFF_CD   |   TIE_CASES_PER_ROW | LCT_REQD_FLG   | BY_TRANS_MODE   |   GS_MGR_ID | TRACEABLE_TYPE   |   PARNT_ITEM_NUM |   BY_UOM_CONV_TO_PARNT | OVERPACK_FLG   |   AUDIT_CRTE_ID | AUDIT_CRTE_DT              |   AUDIT_UPD_ID | AUDIT_UPD_DT               |
|----------------------:|--------------:|:-----------------------------------------------------------|--------------------:|---------------------:|------------------:|:-----------------------------|:------------------------|:--------------|:-----------------------------|:-------------------|-------------------------:|:-----------|:----------|:-----------|:------------|:-------------|--------------------------:|----------------------:|------------------------:|-----------------------:|--------------------------:|:---------------|:----------------|:--------------|:---------------|:------------------|:-------------------|:-----------|:------------|:---------------|:----------------|:--------------|:--------------------|:-----------------|:------------------|:----------------|:----------|------------------:|---------------:|:----------------|:------------|:-----------|:------------------|:----------|:---------------|:---------------|:----------------|:---------------|:----------------|:-----------------|:-----------|:----------|---------:|:--------------|:----------------|-------------:|:----------------------|:--------------------|:--------------|:---------------|:----------|---------------:|:-----------|:---------------------|:----------------|:-----------------|:----------------|:-----------------|:----------------|:--------------------|:------------------------------|:---------------------|:-------------------------------|:-------------------|:-----------------------|:-------------------------|:---------------------------|:------------------------------|:-------------------------------|:-------------------------------|:--------------------------------|:--------------------|:--------------------|:-----------------|:-----------------|:-------------------|:------------------------|---------------------:|:----------------------|:---------------|-----------------:|:---------------|:----------------|:--------------------|:------------------|--------------------:|------------------:|:---------------|:----------------|:------------|:-------------|:--------------|:-----------------|:----------|:----------|:----------|:--------------|:--------------|----------------:|-----------------------:|---------------------:|:------------|--------------------:|:---------------|:----------------|------------:|:-----------------|-----------------:|-----------------------:|:---------------|----------------:|:---------------------------|---------------:|:---------------------------|
|           4.11953e+07 |   1.27338e+06 | CART. COMPOSITE ROAM3 ALUM CATH/GEN SUPPLY 28.75X57X75.25" |              122154 |                 6242 |       4.30168e+06 | STERIS CORPORATION EQUIPMENT | VB                      | STRSEQ        | STERIS CORPORATION EQUIPMENT | STERIS CORPORATION |                  6821.8  | SXSR3G3CG  | EA        | EA         | EA          | N/A          |                       nan |                     1 |                       1 |                      1 |                       nan | K              | CUSTOM          | MS            | MEDSURG        | MS                | MEDSURG            | N/A        | N/A         | N/A            | N/A             | N             | 9999-12-31 00:00:00 | N/A              | N/A               | N               |           |           6821.8  |            nan | N               | N           | N          |                   |           | N/A            | N/A            | N/A             | N/A            | N/A             | -                | N          | N         |    97104 | N/A           | P               |         2559 | STERIS CORP EQUIPMENT | N/A                 | IN30          | N/A            | N         |             52 | N/A        | N/A                  | B               | Blue             | N/A             | N/A              | N/ NK           | F05                 | Equipment & Equip Disposables | C027                 | Exam & Patient Room Furnishing | G0060              | Carts / Stands / Trays | S01737                   | Service/Utility Carts      | EQ                            | EQUIPMENT                      | EQUIPMENT                      | LAB SUPPLIES                    | NaT                 | NaT                 | N/A              | N                | Z                  | NTWK NSTK-NO SLS-NO INV |                    4 | MID - HIGH RISK       | EA             |              nan | N/A            | N/A             | 9999-12-31 00:00:00 | N                 |                 nan |               nan |                | N               |             |              |               | N                | N         | N         | N         | MMS           | N/A           |               0 |                      0 |                    0 | N/A         |                   0 | N              | N/A             |           0 | N/A              |                0 |                      0 | N              |          843669 | 2025-06-12 00:26:49.378000 |         872231 | 2025-09-19 14:16:07.294000 |
|           4.11957e+07 |   1.27338e+06 | INSTALLATION, F/BASE & UPPER CABINETS                      |              122154 |                 6242 |       4.30168e+06 | STERIS CORPORATION EQUIPMENT | VB                      | STRSEQ        | STERIS CORPORATION EQUIPMENT | STERIS CORPORATION |                   549.63 | SE133112   | EA        | EA         | EA          | N/A          |                       nan |                     1 |                       1 |                      1 |                       nan | K              | CUSTOM          | MS            | MEDSURG        | MS                | MEDSURG            | N/A        | N/A         | N/A            | N/A             | N             | 9999-12-31 00:00:00 | N/A              | N/A               | N               |           |            549.63 |            nan | N               | N           | N          |                   |           | N/A            | N/A            | N/A             | N/A            | N/A             | -                | N          | N         |    98901 | N/A           | P               |         2559 | STERIS CORP EQUIPMENT | N/A                 | IN30          | N/A            | N         |             52 | N/A        | N/A                  | B               | Blue             | N/A             | N/A              | N/ NK           | F05                 | Equipment & Equip Disposables | C027                 | Exam & Patient Room Furnishing | G0271              | Other Fixtures         | S01911                   | Surgical Room Furn & Equip | EQ                            | EQUIPMENT                      | EQUIPMENT                      | LAB SUPPLIES                    | NaT                 | NaT                 | N/A              | N                | Z                  | NTWK NSTK-NO SLS-NO INV |                    4 | MID - HIGH RISK       | EA             |              nan | N/A            | N/A             | 9999-12-31 00:00:00 | N                 |                 nan |               nan |                | N               |             |              |               | N                | N         | N         | N         | MMS           | N/A           |               0 |                      0 |                    0 | N/A         |                   0 | N              | N/A             |           0 | N/A              |                0 |                      0 | N              |          843669 | 2025-06-12 00:26:49.378000 |         872231 | 2025-09-19 14:16:07.294000 |
|           4.11967e+07 |   1.27338e+06 | CART, COMPOSITE ROAM2 ALUM GENSTRG 28.75X37.5X44.25"       |              122154 |                 6242 |       4.30168e+06 | STERIS CORPORATION EQUIPMENT | VB                      | STRSEQ        | STERIS CORPORATION EQUIPMENT | STERIS CORPORATION |                  2847.02 | SXSR2CGGS  | EA        | EA         | EA          | N/A          |                       nan |                     1 |                       1 |                      1 |                       nan | K              | CUSTOM          | MS            | MEDSURG        | MS                | MEDSURG            | N/A        | N/A         | N/A            | N/A             | N             | 9999-12-31 00:00:00 | N/A              | N/A               | N               |           |           2847.02 |            nan | N               | N           | N          |                   |           | N/A            | N/A            | N/A             | N/A            | N/A             | -                | N          | N         |    97104 | N/A           | P               |         2559 | STERIS CORP EQUIPMENT | N/A                 | IN30          | N/A            | N         |             52 | N/A        | N/A                  | B               | Blue             | N/A             | N/A              | N/ NK           | F05                 | Equipment & Equip Disposables | C027                 | Exam & Patient Room Furnishing | G0060              | Carts / Stands / Trays | S01737                   | Service/Utility Carts      | EQ                            | EQUIPMENT                      | EQUIPMENT                      | LAB SUPPLIES                    | NaT                 | NaT                 | N/A              | N                | Z                  | NTWK NSTK-NO SLS-NO INV |                    4 | MID - HIGH RISK       | EA             |              nan | N/A            | N/A             | 9999-12-31 00:00:00 | N                 |                 nan |               nan |                | N               |             |              |               | N                | N         | N         | N         | MMS           | N/A           |               0 |                      0 |                    0 | N/A         |                   0 | N              | N/A             |           0 | N/A              |                0 |                      0 | N              |          843669 | 2025-06-12 00:26:49.378000 |         872231 | 2025-09-19 14:16:07.294000 |

### `v_fct_sales_2425_revised` _(partitioned dataset, 115 shards)_

- **Folder:** `C:\Users\maina\Desktop\Capstone\data_raw\v_fct_sales_2425_revised`
- **Total size:** 1510.52 MB
- **Rows:** 57,755,638
- **Columns:** 14

**Columns:**

| # | Column | Type | Nulls | Stats |
|---|---|---|---|---|
| 1 | `DIM_ITEM_E1_CURR_ID` | DECIMAL(38,0) | 0 (0.0%) | min=0, max=39051823, mean=2447419.1864, median=508401 |
| 2 | `DIM_CUST_CURR_ID` | DECIMAL(38,0) | 0 (0.0%) | min=33741, max=429678777, mean=47169489.9696, median=5827568 |
| 3 | `SHIP_QTY` | DECIMAL(38,10) | 0 (0.0%) | min=-441600.0000000000, max=920580.0000000000, mean=3.6402, median=1.0000000000 |
| 4 | `PRMRY_UOM` | VARCHAR | 0 (0.0%) | distinct=45; top: `EA` (41,918,485), `*` (4,201,108), `PK` (2,428,693), `BX` (1,741,397), `BT` (1,711,540) |
| 5 | `PRMRY_QTY` | DECIMAL(38,10) | 0 (0.0%) | min=-1000000.0000000000, max=3552000.0000000000, mean=297.465, median=13.0000000000 |
| 6 | `ORDR_QTY` | DECIMAL(38,10) | 0 (0.0%) | min=-441600.0000000000, max=920580.0000000000, mean=3.6402, median=1.0000000000 |
| 7 | `UNIT_SLS_AMT` | DECIMAL(38,10) | 0 (0.0%) | min=0E-10, max=72834686.0000000000, mean=74.2095, median=20.2300000000 |
| 8 | `ORDR_NUM` | VARCHAR | 0 (0.0%) | distinct=9,962,697; top: (skipped — high cardinality) |
| 9 | `ORDR_LINE_NUM` | DECIMAL(38,10) | 0 (0.0%) | min=0.0010000000, max=931.0000000000, mean=9.7123, median=5.0000000000 |
| 10 | `INV_NUM` | VARCHAR | 0 (0.0%) | distinct=14,588,558; top: (skipped — high cardinality) |
| 11 | `MCK_FLG` | VARCHAR | 0 (0.0%) | distinct=2; top: `N` (38,472,642), `Y` (19,282,996) |
| 12 | `ORDR_SRC_DSC` | VARCHAR | 0 (0.0%) | distinct=6; top: `ONLINE` (33,689,209), `CONNECTIVITY` (13,864,561), `CUSTOMER SERVICE` (5,096,066), `N/A` (3,348,216), `REP` (1,650,717) |
| 13 | `ORDR_MTHD_DSC` | VARCHAR | 0 (0.0%) | distinct=12; top: `SUPPLY MANAGER` (33,333,639), `EDI` (8,959,430), `CUSTOMER SERVICE` (5,017,970), `B2B` (4,905,131), `N/A` (3,348,216) |
| 14 | `DIM_ORDR_DT_ID` | DECIMAL(38,0) | 0 (0.0%) | min=20240101, max=20250101, mean=20240664.4619, median=20240629 |

**Sample (first 3 rows):**

|   DIM_ITEM_E1_CURR_ID |   DIM_CUST_CURR_ID |   SHIP_QTY | PRMRY_UOM   |   PRMRY_QTY |   ORDR_QTY |   UNIT_SLS_AMT |   ORDR_NUM |   ORDR_LINE_NUM |   INV_NUM | MCK_FLG   | ORDR_SRC_DSC   | ORDR_MTHD_DSC   |   DIM_ORDR_DT_ID |
|----------------------:|-------------------:|-----------:|:------------|------------:|-----------:|---------------:|-----------:|----------------:|----------:|:----------|:---------------|:----------------|-----------------:|
|           3.19039e+07 |        2.16758e+06 |          1 | EA          |        2000 |          1 |         364.03 |   20127361 |               3 |  66008794 | Y         | ONLINE         | SUPPLY MANAGER  |      2.02404e+07 |
|      485786           |   988856           |          3 | EA          |           9 |          3 |           7.08 |   20089964 |               1 |  65993105 | N         | CONNECTIVITY   | EDI             |      2.02404e+07 |
|      894332           |        7.40904e+06 |          1 | EA          |         100 |          1 |        1213.21 |   20144313 |               4 |  21989193 | N         | CONNECTIVITY   | EDI             |      2.02404e+07 |

### `v_fct_sales_2526_revised` _(partitioned dataset, 122 shards)_

- **Folder:** `C:\Users\maina\Desktop\Capstone\data_raw\v_fct_sales_2526_revised`
- **Total size:** 1641.37 MB
- **Rows:** 57,727,830
- **Columns:** 14

**Columns:**

| # | Column | Type | Nulls | Stats |
|---|---|---|---|---|
| 1 | `DIM_ITEM_E1_CURR_ID` | DECIMAL(38,0) | 0 (0.0%) | min=0, max=43463872, mean=3634036.7557, median=546541 |
| 2 | `DIM_CUST_CURR_ID` | DECIMAL(38,0) | 0 (0.0%) | min=33741, max=562150952, mean=73979593.7743, median=6401938 |
| 3 | `SHIP_QTY` | DECIMAL(38,10) | 0 (0.0%) | min=-9000.0000000000, max=6170700.0000000000, mean=4.0015, median=1.0000000000 |
| 4 | `PRMRY_UOM` | VARCHAR | 0 (0.0%) | distinct=43; top: `EA` (42,078,026), `*` (3,990,389), `PK` (2,331,765), `BX` (1,761,052), `BT` (1,702,632) |
| 5 | `PRMRY_QTY` | DECIMAL(38,10) | 0 (0.0%) | min=-900000.0000000000, max=6170700.0000000000, mean=315.4813, median=16.0000000000 |
| 6 | `ORDR_QTY` | DECIMAL(38,10) | 0 (0.0%) | min=-9000.0000000000, max=6170700.0000000000, mean=4.0015, median=1.0000000000 |
| 7 | `UNIT_SLS_AMT` | DECIMAL(38,10) | 0 (0.0%) | min=0E-10, max=1342764857.1400000000, mean=631.2026, median=20.8000000000 |
| 8 | `ORDR_NUM` | VARCHAR | 0 (0.0%) | distinct=9,993,914; top: (skipped — high cardinality) |
| 9 | `ORDR_LINE_NUM` | DECIMAL(38,10) | 0 (0.0%) | min=1.0000000000, max=907.0000000000, mean=10.2357, median=5.0000000000 |
| 10 | `INV_NUM` | VARCHAR | 0 (0.0%) | distinct=13,700,264; top: (skipped — high cardinality) |
| 11 | `MCK_FLG` | VARCHAR | 0 (0.0%) | distinct=2; top: `N` (37,760,496), `Y` (19,967,334) |
| 12 | `ORDR_SRC_DSC` | VARCHAR | 0 (0.0%) | distinct=6; top: `ONLINE` (33,493,993), `CONNECTIVITY` (14,987,817), `CUSTOMER SERVICE` (6,012,056), `N/A` (2,954,258), `REP` (179,187) |
| 13 | `ORDR_MTHD_DSC` | VARCHAR | 0 (0.0%) | distinct=12; top: `SUPPLY MANAGER` (33,187,928), `EDI` (9,940,159), `CUSTOMER SERVICE` (5,959,786), `B2B` (5,047,658), `N/A` (2,954,258) |
| 14 | `DIM_ORDR_DT_ID` | DECIMAL(38,0) | 0 (0.0%) | min=20250101, max=20260101, mean=20250666.7236, median=20250701 |

**Sample (first 3 rows):**

|   DIM_ITEM_E1_CURR_ID |   DIM_CUST_CURR_ID |   SHIP_QTY | PRMRY_UOM   |   PRMRY_QTY |   ORDR_QTY |   UNIT_SLS_AMT |   ORDR_NUM |   ORDR_LINE_NUM |   INV_NUM | MCK_FLG   | ORDR_SRC_DSC   | ORDR_MTHD_DSC   |   DIM_ORDR_DT_ID |
|----------------------:|-------------------:|-----------:|:------------|------------:|-----------:|---------------:|-----------:|----------------:|----------:|:----------|:---------------|:----------------|-----------------:|
|                486974 |        7.76482e+06 |          2 | EA          |         400 |          2 |          17    |   39430405 |              21 |  63033708 | Y         | CONNECTIVITY   | EDI             |      2.02501e+07 |
|                 22430 |        7.76482e+06 |          4 | EA          |         400 |          4 |           3.66 |   39430405 |               4 |  63033708 | N         | CONNECTIVITY   | EDI             |      2.02501e+07 |
|                272868 |        5.40423e+06 |          1 | *           |           0 |          1 |           1.19 |   39443995 |               2 |  73999966 | N         | CONNECTIVITY   | EDI             |      2.02501e+07 |

---

## Layer detail: CLEAN (intermediate)

### `customers_clean`

- **Folder:** `C:\Users\maina\Desktop\Capstone\data_clean\customer`
- **Total size:** 46.74 MB
- **Rows:** 1,134,418
- **Columns:** 17

**Columns:**

| # | Column | Type | Nulls | Stats |
|---|---|---|---|---|
| 1 | `DIM_CUST_CURR_ID` | DOUBLE | 0 (0.0%) | min=32757.0, max=593783882.0, mean=152892627.2991, median=12197400.0 |
| 2 | `CUST_NUM` | DOUBLE | 0 (0.0%) | min=51559.0, max=98871720.0, mean=54559318.3996, median=59122948.5 |
| 3 | `CUST_NAME` | VARCHAR | 0 (0.0%) | distinct=725,252; top: (skipped — high cardinality) |
| 4 | `CUST_TYPE_CD` | VARCHAR | 619 (0.1%) | distinct=3; top: `S` (860,893), `B` (219,662), `X` (53,244) |
| 5 | `CUST_TYPE_DSC` | VARCHAR | 0 (0.0%) | distinct=4; top: `SHIP TO ADDRESS ONLY` (860,893), `BILL TO ADDRESS ONLY` (219,662), `BILL TO AND SHIP TO ADDRESS` (53,244), `N/A` (619) |
| 6 | `SPCLTY_CD` | VARCHAR | 0 (0.0%) | distinct=278; top: `FP` (101,555), `M04` (60,902), `SKL` (53,281), `M14` (41,106), `M07` (41,036) |
| 7 | `SPCLTY_DSC` | VARCHAR | 190 (0.0%) | distinct=274; top: `FAMILY PRACTICE` (101,555), `OTHER` (60,902), `SKILLED` (53,281), `HOME MEDICAL EQUIPMENT` (41,106), `MULTIPLE SPECIALTY GROUP PRACT` (41,036) |
| 8 | `MKT_CD` | VARCHAR | 0 (0.0%) | distinct=8; top: `PO` (788,758), `LTC` (203,684), `HC` (61,784), `SC` (40,316), `LC` (33,486) |
| 9 | `MMS_CLASS_CD` | VARCHAR | 0 (0.0%) | distinct=5; top: `B` (751,491), `D` (266,300), `G` (116,604), `A` (16), `N/A` (7) |
| 10 | `MMS_CLASS_DSC` | VARCHAR | 0 (0.0%) | distinct=5; top: `PRIMARY CARE` (751,489), `LONG TERM CARE` (266,300), `OTHER` (116,604), `ACUTE CARE` (16), `N/A` (9) |
| 11 | `MMS_SGMNT_CD` | VARCHAR | 0 (0.0%) | distinct=13; top: `14` (788,974), `10` (187,054), `06` (61,455), `17` (40,043), `35` (33,492) |
| 12 | `MMS_SUB_CLASS_CD` | VARCHAR | 0 (0.0%) | distinct=39; top: `14` (501,853), `10` (179,785), `06` (58,760), `11` (51,395), `17` (36,125) |
| 13 | `ACTV_FLG` | VARCHAR | 0 (0.0%) | distinct=1; top: `Y` (1,134,418) |
| 14 | `ZIP` | VARCHAR | 269 (0.0%) | distinct=347,228; top: (skipped — high cardinality) |
| 15 | `STATE` | VARCHAR | 9 (0.0%) | distinct=64; top: `CA` (117,696), `TX` (103,567), `FL` (90,690), `NY` (51,513), `PA` (50,007) |
| 16 | `CITY` | VARCHAR | 12 (0.0%) | distinct=14,449; top: `HOUSTON` (12,822), `NEW YORK` (7,370), `MIAMI` (7,232), `SAN ANTONIO` (6,891), `PHOENIX` (6,616) |
| 17 | `CNTRY_CD` | VARCHAR | 0 (0.0%) | distinct=5; top: `US` (1,134,413), `VI` (2), `BS` (1), `CA` (1), `PR` (1) |

**Sample (first 3 rows):**

|   DIM_CUST_CURR_ID |    CUST_NUM | CUST_NAME                  | CUST_TYPE_CD   | CUST_TYPE_DSC               | SPCLTY_CD   | SPCLTY_DSC      | MKT_CD   | MMS_CLASS_CD   | MMS_CLASS_DSC   |   MMS_SGMNT_CD |   MMS_SUB_CLASS_CD | ACTV_FLG   |   ZIP | STATE   | CITY         | CNTRY_CD   |
|-------------------:|------------:|:---------------------------|:---------------|:----------------------------|:------------|:----------------|:---------|:---------------|:----------------|---------------:|-------------------:|:-----------|------:|:--------|:-------------|:-----------|
|        1.54666e+06 | 4.98001e+06 | NEW LIFE SURGICAL ASC      | B              | BILL TO ADDRESS ONLY        | FP          | FAMILY PRACTICE | PO       | B              | PRIMARY CARE    |             14 |                 14 | Y          | 32216 | FL      | JACKSONVILLE | US         |
|        1.52002e+07 | 7.25001e+07 | J HALL ENTERPRISE LLC      | B              | BILL TO ADDRESS ONLY        | FP          | FAMILY PRACTICE | PO       | B              | PRIMARY CARE    |             14 |                 12 | Y          | 85251 | AZ      | SCOTTSDALE   | US         |
|        1.59762e+06 | 5.00568e+06 | UNIVERSITY EYE SPECIALISTS | X              | BILL TO AND SHIP TO ADDRESS | OPH         | OPHTHALMOLOGY   | PO       | B              | PRIMARY CARE    |             14 |                 21 | Y          | 14569 | NY      | WARSAW       | US         |

### `features` _(partitioned dataset, 3 shards)_

- **Folder:** `C:\Users\maina\Desktop\Capstone\data_clean\features`
- **Total size:** 27.69 MB
- **Rows:** 778,720
- **Columns:** 53

**Columns:**

| # | Column | Type | Nulls | Stats |
|---|---|---|---|---|
| 1 | `DIM_CUST_CURR_ID` | DOUBLE | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 2 | `recency_days` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 3 | `frequency` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 4 | `monetary` | DOUBLE | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 5 | `avg_order_gap_days` | DOUBLE | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 6 | `R_score` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 7 | `F_score` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 8 | `M_score` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 9 | `RFM_score` | VARCHAR | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 10 | `churn_label` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 11 | `avg_revenue_per_order` | DOUBLE | 0 (0.0%) | min=0.0, max=1540000.0, mean=513.2387, median=203.23 |
| 12 | `n_categories_bought` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 13 | `category_hhi` | DOUBLE | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 14 | `cycle_regularity` | DOUBLE | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 15 | `CUST_TYPE_CD` | VARCHAR | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 16 | `SPCLTY_CD` | VARCHAR | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 17 | `MKT_CD` | VARCHAR | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 18 | `MMS_CLASS_CD` | VARCHAR | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 19 | `STATE` | VARCHAR | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 20 | `specialty_tier` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 21 | `pct_of_total_revenue` | DOUBLE | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 22 | `spec_avg_revenue_per_order` | DOUBLE | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 23 | `cust_type_encoded` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 24 | `mkt_cd_encoded` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 25 | `mms_class_encoded` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 26 | `state_grouped` | VARCHAR | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 27 | `state_encoded` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 28 | `spec_FP` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 29 | `spec_SKL` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 30 | `spec_M07` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 31 | `spec_PD` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 32 | `spec_CHC` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 33 | `spec_IM` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 34 | `spec_M16` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 35 | `spec_HIA` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 36 | `spec_SC` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 37 | `spec_D` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 38 | `spec_O` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 39 | `spec_OBG` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 40 | `spec_GS` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 41 | `spec_ON` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 42 | `spec_HL` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 43 | `spec_R` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 44 | `spec_EM` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 45 | `spec_GP` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 46 | `spec_M04` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 47 | `spec_M14` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 48 | `specialty_revenue_trend_pct` | DOUBLE | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 49 | `supplier_profile` | VARCHAR | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 50 | `median_monthly_spend` | DOUBLE | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 51 | `active_months_last_12` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 52 | `size_tier` | VARCHAR | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 53 | `affordability_ceiling` | DOUBLE | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |

**Sample (first 3 rows):**

|   DIM_CUST_CURR_ID |   recency_days |   frequency |   monetary |   avg_order_gap_days |   R_score |   F_score |   M_score |   RFM_score |   churn_label |   avg_revenue_per_order |   n_categories_bought |   category_hhi |   cycle_regularity | CUST_TYPE_CD   | SPCLTY_CD   | MKT_CD   | MMS_CLASS_CD   | STATE   |   specialty_tier |   pct_of_total_revenue |   spec_avg_revenue_per_order |   cust_type_encoded |   mkt_cd_encoded |   mms_class_encoded | state_grouped   |   state_encoded |   spec_FP |   spec_SKL |   spec_M07 |   spec_PD |   spec_CHC |   spec_IM |   spec_M16 |   spec_HIA |   spec_SC |   spec_D |   spec_O |   spec_OBG |   spec_GS |   spec_ON |   spec_HL |   spec_R |   spec_EM |   spec_GP |   spec_M04 |   spec_M14 |   specialty_revenue_trend_pct | supplier_profile   |   median_monthly_spend |   active_months_last_12 | size_tier   |   affordability_ceiling |
|-------------------:|---------------:|------------:|-----------:|---------------------:|----------:|----------:|----------:|------------:|--------------:|------------------------:|----------------------:|---------------:|-------------------:|:---------------|:------------|:---------|:---------------|:--------|-----------------:|-----------------------:|-----------------------------:|--------------------:|-----------------:|--------------------:|:----------------|----------------:|----------:|-----------:|-----------:|----------:|-----------:|----------:|-----------:|-----------:|----------:|---------:|---------:|-----------:|----------:|----------:|----------:|---------:|----------:|----------:|-----------:|-----------:|------------------------------:|:-------------------|-----------------------:|------------------------:|:------------|------------------------:|
|        1.60167e+07 |              1 |          65 |   198247   |                17.41 |         5 |         5 |         5 |         555 |             0 |                 3049.96 |                     3 |         0.9547 |              10.94 | S              | HIA         | LTC      | D              | MD      |                1 |                 2.9513 |                       428.39 |                   0 |                3 |                   1 | MD              |              19 |         0 |          0 |          0 |         0 |          0 |         0 |          0 |          1 |         0 |        0 |        0 |          0 |         0 |         0 |         0 |        0 |         0 |         0 |          0 |          0 |                          4.16 | mixed              |                6854.85 |                      11 | large       |                13709.7  |
|        1.31387e+06 |              3 |         102 |    22487.5 |                 8.15 |         5 |         5 |         5 |         555 |             0 |                  220.47 |                    12 |         0.217  |               5.55 | X              | RHU         | PO       | B              | AZ      |                1 |                 0.4314 |                       588.66 |                   1 |                4 |                   0 | AZ              |               2 |         0 |          0 |          0 |         0 |          0 |         0 |          0 |          0 |         0 |        0 |        0 |          0 |         0 |         0 |         0 |        0 |         0 |         0 |          0 |          0 |                         -2.83 | mixed              |                 755.09 |                      11 | mid         |                 1359.16 |
|        2.43272e+08 |              1 |         174 |    35572.5 |                 4.19 |         5 |         5 |         5 |         555 |             0 |                  204.44 |                    12 |         0.2378 |               3.32 | S              | M13         | LTC      | D              | MS      |                1 |                 0.7047 |                       183.31 |                   0 |                3 |                   1 | MS              |              24 |         0 |          0 |          0 |         0 |          0 |         0 |          0 |          0 |         0 |        0 |        0 |          0 |         0 |         0 |         0 |        0 |         0 |         0 |          0 |          0 |                          1.84 | mixed              |                1410.38 |                      11 | mid         |                 2538.68 |

### `merged_dataset`

- **Folder:** `C:\Users\maina\Desktop\Capstone\data_clean\serving`
- **Total size:** 7458.05 MB
- **Rows:** 110,402,862
- **Columns:** 38

**Columns:**

| # | Column | Type | Nulls | Stats |
|---|---|---|---|---|
| 1 | `ORDR_NUM` | VARCHAR | 0 (0.0%) | distinct=19,291,448; top: (skipped — high cardinality) |
| 2 | `ORDR_LINE_NUM` | DECIMAL(38,10) | 0 (0.0%) | min=0.0010000000, max=931.0000000000, mean=10.0276, median=5.0000000000 |
| 3 | `DIM_CUST_CURR_ID` | DECIMAL(38,0) | 0 (0.0%) | min=33741, max=562150952, mean=60361701.187, median=5928466 |
| 4 | `DIM_ITEM_E1_CURR_ID` | DECIMAL(38,0) | 0 (0.0%) | min=0, max=43463872, mean=2937633.7703, median=525248 |
| 5 | `ORDR_QTY` | DECIMAL(38,10) | 0 (0.0%) | min=0E-10, max=6170700.0000000000, mean=4.0488, median=1.0000000000 |
| 6 | `UNIT_SLS_AMT` | DECIMAL(38,10) | 0 (0.0%) | min=0E-10, max=1540000.0000000000, mean=74.5322, median=22.1100000000 |
| 7 | `DIM_ORDR_DT_ID` | DECIMAL(38,0) | 0 (0.0%) | min=20240101, max=20260101, mean=20245665.6816, median=20250101 |
| 8 | `ORDR_MTHD_DSC` | VARCHAR | 0 (0.0%) | distinct=12; top: `SUPPLY MANAGER` (66,470,098), `EDI` (18,016,834), `CUSTOMER SERVICE` (9,909,775), `B2B` (9,607,208), `N/A` (3,698,821) |
| 9 | `ORDR_SRC_DSC` | VARCHAR | 0 (0.0%) | distinct=6; top: `ONLINE` (67,131,555), `CONNECTIVITY` (27,624,042), `CUSTOMER SERVICE` (10,040,052), `N/A` (3,669,336), `REP` (1,730,646) |
| 10 | `MCK_FLG` | VARCHAR | 0 (0.0%) | distinct=2; top: `N` (71,541,533), `Y` (38,861,329) |
| 11 | `INV_NUM` | VARCHAR | 0 (0.0%) | distinct=26,985,754; top: (skipped — high cardinality) |
| 12 | `SHIP_QTY` | DECIMAL(38,10) | 0 (0.0%) | min=0E-10, max=6170700.0000000000, mean=4.0488, median=1.0000000000 |
| 13 | `PRMRY_QTY` | DECIMAL(38,10) | 0 (0.0%) | min=0E-10, max=6170700.0000000000, mean=323.9521, median=20.0000000000 |
| 14 | `fiscal_year` | VARCHAR | 0 (0.0%) | distinct=2; top: `FY2425` (55,215,419), `FY2526` (55,187,443) |
| 15 | `order_year` | INTEGER | 0 (0.0%) | min=2024, max=2026, mean=2024.5003, median=2025.0 |
| 16 | `order_month` | INTEGER | 0 (0.0%) | min=1, max=12, mean=6.4755, median=7.0 |
| 17 | `order_day` | INTEGER | 0 (0.0%) | min=1, max=31, mean=15.2741, median=15.0 |
| 18 | `CUST_TYPE_CD` | VARCHAR | 0 (0.0%) | distinct=3; top: `S` (97,597,724), `X` (12,772,758), `B` (32,380) |
| 19 | `CUST_TYPE_DSC` | VARCHAR | 0 (0.0%) | distinct=3; top: `SHIP TO ADDRESS ONLY` (97,597,724), `BILL TO AND SHIP TO ADDRESS` (12,772,758), `BILL TO ADDRESS ONLY` (32,380) |
| 20 | `SPCLTY_CD` | VARCHAR | 0 (0.0%) | distinct=272; top: `SKL` (14,480,543), `FP` (10,184,564), `M07` (5,129,412), `M16` (4,185,683), `HH` (4,107,052) |
| 21 | `SPCLTY_DSC` | VARCHAR | 1,735 (0.0%) | distinct=267; top: `SKILLED` (14,480,543), `FAMILY PRACTICE` (10,184,564), `MULTIPLE SPECIALTY GROUP PRACT` (5,129,412), `URGENT CARE` (4,185,683), `HOME HOSPICE` (4,107,052) |
| 22 | `MKT_CD` | VARCHAR | 0 (0.0%) | distinct=7; top: `PO` (62,350,364), `LTC` (29,794,752), `SC` (10,838,301), `HC` (5,020,039), `LC` (1,887,510) |
| 23 | `MMS_CLASS_CD` | VARCHAR | 0 (0.0%) | distinct=4; top: `B` (73,301,975), `D` (34,814,645), `G` (2,285,522), `N/A` (720) |
| 24 | `MMS_CLASS_DSC` | VARCHAR | 0 (0.0%) | distinct=4; top: `PRIMARY CARE` (73,301,975), `LONG TERM CARE` (34,814,645), `OTHER` (2,285,522), `N/A` (720) |
| 25 | `ZIP` | VARCHAR | 6,662 (0.0%) | distinct=176,096; top: (skipped — high cardinality) |
| 26 | `STATE` | VARCHAR | 0 (0.0%) | distinct=55; top: `CA` (12,776,181), `TX` (10,318,744), `FL` (9,552,522), `PA` (4,721,929), `VA` (4,375,480) |
| 27 | `CITY` | VARCHAR | 1 (0.0%) | distinct=11,444; top: `RICHMOND` (1,806,295), `HOUSTON` (952,162), `SAN ANTONIO` (922,881), `PHOENIX` (716,622), `LAS VEGAS` (648,832) |
| 28 | `ITEM_DSC` | VARCHAR | 6,906,570 (6.3%) | distinct=161,968; top: (skipped — high cardinality) |
| 29 | `is_private_brand` | BIGINT | 6,906,570 (6.3%) | min=0, max=1, mean=0.3728, median=0.0 |
| 30 | `PRVT_BRND_FLG` | VARCHAR | 6,906,570 (6.3%) | distinct=2; top: `N` (64,915,717), `Y` (38,580,575) |
| 31 | `PROD_FMLY_LVL1_CD` | VARCHAR | 6,906,570 (6.3%) | distinct=18; top: `F01` (20,366,598), `F03` (14,386,918), `F08` (11,324,696), `F02` (10,808,741), `F13` (10,752,858) |
| 32 | `PROD_FMLY_LVL1_DSC` | VARCHAR | 6,906,570 (6.3%) | distinct=18; top: `Nursing and Surgical Supplies` (20,366,598), `Infection Prevention` (14,386,918), `Wound Care & Skin Care` (11,324,696), `Rx` (10,808,741), `Fee` (10,752,858) |
| 33 | `PROD_CTGRY_LVL2_CD` | VARCHAR | 6,906,570 (6.3%) | distinct=105; top: `C082` (10,574,403), `C040` (5,782,579), `C033` (5,670,419), `C057` (4,356,699), `C050` (4,353,978) |
| 34 | `PROD_CTGRY_LVL2_DSC` | VARCHAR | 6,906,570 (6.3%) | distinct=105; top: `Distribution Fees` (10,574,403), `IV Therapy` (5,782,579), `Gloves` (5,670,419), `Rx-Otc And Topicals` (4,356,699), `Needles & Syringes` (4,353,978) |
| 35 | `SUPLR_DSC` | VARCHAR | 6,906,570 (6.3%) | distinct=2,048; top: `MCKESSON MEDICAL SURGICAL` (37,532,486), `SPECIAL HANDLING` (6,943,305), `N/A` (3,462,108), `BECTON DICKINSON` (3,331,513), `CARDINAL HEALTHCARE` (2,207,134) |
| 36 | `SUPLR_ROLLUP_DSC` | VARCHAR | 6,906,570 (6.3%) | distinct=1,862; top: `CYPRESS MEDICAL PRODUCTS LTD` (24,408,070), `SPECIAL HANDLING` (6,943,305), `BECTON DICKINSON` (3,719,774), `N/A` (3,462,108), `CARDINAL HEALTH` (2,857,030) |
| 37 | `is_discontinued` | BIGINT | 6,906,570 (6.3%) | min=0, max=0, mean=0.0, median=0.0 |
| 38 | `specialty_tier` | BIGINT | 0 (0.0%) | min=1, max=3, mean=1.081, median=1.0 |

**Sample (first 3 rows):**

|   ORDR_NUM |   ORDR_LINE_NUM |   DIM_CUST_CURR_ID |   DIM_ITEM_E1_CURR_ID |   ORDR_QTY |   UNIT_SLS_AMT |   DIM_ORDR_DT_ID | ORDR_MTHD_DSC   | ORDR_SRC_DSC   | MCK_FLG   |   INV_NUM |   SHIP_QTY |   PRMRY_QTY | fiscal_year   |   order_year |   order_month |   order_day | CUST_TYPE_CD   | CUST_TYPE_DSC        | SPCLTY_CD   | SPCLTY_DSC         | MKT_CD   | MMS_CLASS_CD   | MMS_CLASS_DSC   |       ZIP | STATE   | CITY           | ITEM_DSC                                                  |   is_private_brand | PRVT_BRND_FLG   | PROD_FMLY_LVL1_CD   | PROD_FMLY_LVL1_DSC   | PROD_CTGRY_LVL2_CD   | PROD_CTGRY_LVL2_DSC   | SUPLR_DSC              | SUPLR_ROLLUP_DSC           |   is_discontinued |   specialty_tier |
|-----------:|----------------:|-------------------:|----------------------:|-----------:|---------------:|-----------------:|:----------------|:---------------|:----------|----------:|-----------:|------------:|:--------------|-------------:|--------------:|------------:|:---------------|:---------------------|:------------|:-------------------|:---------|:---------------|:----------------|----------:|:--------|:---------------|:----------------------------------------------------------|-------------------:|:----------------|:--------------------|:---------------------|:---------------------|:----------------------|:-----------------------|:---------------------------|------------------:|-----------------:|
|   36989079 |               6 |        1.17158e+06 |      223277           |          1 |           9.85 |      2.02412e+07 | SUPPLY MANAGER  | ONLINE         | N         |  73238555 |          1 |           0 | FY2425        |         2024 |            12 |           5 | S              | SHIP TO ADDRESS ONLY | EM          | EMERGENCY MEDICINE | PO       | B              | PRIMARY CARE    |     63555 | MO      | MEMPHIS        | HANDLING CHARGESPECHC                                     |                  0 | N               | F13                 | Fee                  | C082                 | Distribution Fees     | SPECIAL HANDLING       | SPECIAL HANDLING           |                 0 |                1 |
|   37045996 |               1 |        6.60445e+06 |           3.86375e+07 |          3 |         890    |      2.02412e+07 | SUPPLY MANAGER  | ONLINE         | N         |  73102379 |          3 |          30 | FY2425        |         2024 |            12 |           2 | S              | SHIP TO ADDRESS ONLY | IM          | INTERNAL MEDICINE  | PO       | B              | PRIMARY CARE    |     60120 | IL      | ELGIN          | MODERNA 2024, COVID VACCINE SYR 0.5ML 12 YRS+ (10DOSE/BX) |                  0 | N               | F02                 | Rx                   | C113                 | Rx-Core Vaccines      | MODERNA US INC         | MODERNA US INC             |                 0 |                1 |
|   37050638 |               4 |        8.68893e+06 |      180534           |          1 |          27.25 |      2.02412e+07 | SUPPLY MANAGER  | ONLINE         | N         |  58821499 |          1 |          60 | FY2425        |         2024 |            12 |           2 | S              | SHIP TO ADDRESS ONLY | SKL         | SKILLED            | LTC      | D              | LONG TERM CARE  | 479061431 | IN      | WEST LAFAYETTE | BRIEF, PER-FIT XLG (15/BG)FIRSTQ                          |                  0 | N               | F06                 | Incontinence         | C013                 | Incontinence Briefs   | FIRST QUALITY PRODUCTS | FIRST QUALITY PRODUCTS LLC |                 0 |                1 |

### `products_clean`

- **Folder:** `C:\Users\maina\Desktop\Capstone\data_clean\product`
- **Total size:** 14.72 MB
- **Rows:** 277,203
- **Columns:** 21

**Columns:**

| # | Column | Type | Nulls | Stats |
|---|---|---|---|---|
| 1 | `DIM_ITEM_E1_CURR_ID` | DOUBLE | 0 (0.0%) | min=-1.0, max=44477195.0, mean=9306595.848, median=807150.0 |
| 2 | `ITEM_E1_NUM` | DOUBLE | 0 (0.0%) | min=-1.0, max=1291533.0, mean=955251.2351, median=1052457.0 |
| 3 | `ITEM_DSC` | VARCHAR | 0 (0.0%) | distinct=270,541; top: (skipped — high cardinality) |
| 4 | `PRVT_BRND_FLG` | VARCHAR | 0 (0.0%) | distinct=2; top: `N` (272,848), `Y` (4,355) |
| 5 | `PROD_FMLY_LVL1_CD` | VARCHAR | 0 (0.0%) | distinct=18; top: `F01` (80,649), `F05` (68,490), `F12` (21,688), `F10` (20,213), `F11` (15,432) |
| 6 | `PROD_FMLY_LVL1_DSC` | VARCHAR | 0 (0.0%) | distinct=18; top: `Nursing and Surgical Supplies` (80,649), `Equipment & Equip Disposables` (68,490), `Lab-Ancillary Lab Products` (21,688), `Lab-Non-Waived Lab` (20,213), `Office and Facility Supplies` (15,432) |
| 7 | `PROD_CTGRY_LVL2_CD` | VARCHAR | 0 (0.0%) | distinct=107; top: `C072` (27,255), `C055` (18,684), `C027` (17,010), `C044` (11,941), `C008` (10,808) |
| 8 | `PROD_CTGRY_LVL2_DSC` | VARCHAR | 0 (0.0%) | distinct=107; top: `Surgical Instruments` (27,255), `Orthopedics` (18,684), `Exam & Patient Room Furnishing` (17,010), `Lab-Lab Supplies` (11,941), `Beds and Patient Safety` (10,808) |
| 9 | `PROD_GRP_LVL3_CD` | VARCHAR | 0 (0.0%) | distinct=531; top: `G0124` (19,583), `G0164` (9,396), `G0068` (7,724), `G0654` (7,012), `G0100` (6,288) |
| 10 | `PROD_GRP_LVL3_DSC` | VARCHAR | 0 (0.0%) | distinct=529; top: `Non-Sterile Surg Instruments` (19,583), `Glassware, Plasticware & Consu` (9,396), `Chemistry Reagents` (7,724), `Soft Goods - Lower Extrem` (7,012), `Custom` (6,288) |
| 11 | `PROD_SUB_CTGRY_LVL4_CD` | VARCHAR | 0 (0.0%) | distinct=2,425; top: `S00634` (5,227), `S00379` (4,367), `S01727` (2,966), `S02191` (2,849), `S01397` (2,705) |
| 12 | `PROD_SUB_CTGRY_LVL4_DSC` | VARCHAR | 0 (0.0%) | distinct=2,410; top: `Exam Tables/Procedure Chairs` (5,227), `Chemistry Reagents` (4,367), `Scrubs, Reusable` (2,966), `Labels/Signs` (2,849), `Other Lab Consumables` (2,705) |
| 13 | `SUPLR_DSC` | VARCHAR | 0 (0.0%) | distinct=2,432; top: `STERIS CORP (CAREFUSION)` (6,023), `MILTEX INSTRUMENT (INTEGRA YK)` (5,127), `MIDMARK CORP` (4,706), `ESSENDANT CO/UNITED STATIONERS` (4,504), `SKLAR INSTRUMENT CORP` (4,174) |
| 14 | `SUPLR_ROLLUP_DSC` | VARCHAR | 0 (0.0%) | distinct=2,207; top: `STERIS CORPORATION` (8,121), `MIDMARK CORP` (5,473), `MILTEX INSTRUMENT (INTEGRA YK)` (5,436), `THERMO SCIENTIFIC (FISHER)` (4,782), `CARDINAL HEALTH` (4,708) |
| 15 | `SLS_GRP_CD` | VARCHAR | 0 (0.0%) | distinct=2,198; top: `2304` (5,583), `4443` (5,474), `4484` (5,151), `5054` (4,691), `1311` (4,506) |
| 16 | `SLS_GRP_DSC` | VARCHAR | 0 (0.0%) | distinct=2,156; top: `CAREFUSION 213 LLC` (5,583), `MIDMARK` (5,474), `MILTEX INSTRUMENT COMPANY INCO` (5,151), `CARDINAL HEALTH` (4,691), `UNITED STATIONERS SUPPLY CO` (4,506) |
| 17 | `GNRC_FLG` | VARCHAR | 0 (0.0%) | distinct=2; top: `N` (271,128), `Y` (6,075) |
| 18 | `DISCTND_FLG` | VARCHAR | 0 (0.0%) | distinct=1; top: `N` (277,203) |
| 19 | `is_private_brand` | BIGINT | 0 (0.0%) | min=0, max=1, mean=0.0157, median=0.0 |
| 20 | `is_discontinued` | BIGINT | 0 (0.0%) | min=0, max=0, mean=0.0, median=0.0 |
| 21 | `is_generic` | BIGINT | 0 (0.0%) | min=0, max=1, mean=0.0219, median=0.0 |

**Sample (first 3 rows):**

|   DIM_ITEM_E1_CURR_ID |   ITEM_E1_NUM | ITEM_DSC                                                   | PRVT_BRND_FLG   | PROD_FMLY_LVL1_CD   | PROD_FMLY_LVL1_DSC            | PROD_CTGRY_LVL2_CD   | PROD_CTGRY_LVL2_DSC            | PROD_GRP_LVL3_CD   | PROD_GRP_LVL3_DSC      | PROD_SUB_CTGRY_LVL4_CD   | PROD_SUB_CTGRY_LVL4_DSC    | SUPLR_DSC                    | SUPLR_ROLLUP_DSC   |   SLS_GRP_CD | SLS_GRP_DSC           | GNRC_FLG   | DISCTND_FLG   |   is_private_brand |   is_discontinued |   is_generic |
|----------------------:|--------------:|:-----------------------------------------------------------|:----------------|:--------------------|:------------------------------|:---------------------|:-------------------------------|:-------------------|:-----------------------|:-------------------------|:---------------------------|:-----------------------------|:-------------------|-------------:|:----------------------|:-----------|:--------------|-------------------:|------------------:|-------------:|
|           4.11953e+07 |   1.27338e+06 | CART. COMPOSITE ROAM3 ALUM CATH/GEN SUPPLY 28.75X57X75.25" | N               | F05                 | Equipment & Equip Disposables | C027                 | Exam & Patient Room Furnishing | G0060              | Carts / Stands / Trays | S01737                   | Service/Utility Carts      | STERIS CORPORATION EQUIPMENT | STERIS CORPORATION |         2559 | STERIS CORP EQUIPMENT | N          | N             |                  0 |                 0 |            0 |
|           4.11957e+07 |   1.27338e+06 | INSTALLATION, F/BASE & UPPER CABINETS                      | N               | F05                 | Equipment & Equip Disposables | C027                 | Exam & Patient Room Furnishing | G0271              | Other Fixtures         | S01911                   | Surgical Room Furn & Equip | STERIS CORPORATION EQUIPMENT | STERIS CORPORATION |         2559 | STERIS CORP EQUIPMENT | N          | N             |                  0 |                 0 |            0 |
|           4.11967e+07 |   1.27338e+06 | CART, COMPOSITE ROAM2 ALUM GENSTRG 28.75X37.5X44.25"       | N               | F05                 | Equipment & Equip Disposables | C027                 | Exam & Patient Room Furnishing | G0060              | Carts / Stands / Trays | S01737                   | Service/Utility Carts      | STERIS CORPORATION EQUIPMENT | STERIS CORPORATION |         2559 | STERIS CORP EQUIPMENT | N          | N             |                  0 |                 0 |            0 |

### `sales` _(partitioned dataset, 4 shards)_

- **Folder:** `C:\Users\maina\Desktop\Capstone\data_clean\sales`
- **Total size:** 4752.02 MB
- **Rows:** 111,453,063
- **Columns:** 17

**Columns:**

| # | Column | Type | Nulls | Stats |
|---|---|---|---|---|
| 1 | `DIM_CUST_CURR_ID` | DECIMAL(38,0) | 0 (0.0%) | min=33741, max=562150952, mean=60519317.7604, median=5928714 |
| 2 | `DIM_ITEM_E1_CURR_ID` | DECIMAL(38,0) | 0 (0.0%) | min=0, max=43463872, mean=2960489.6112, median=525260 |
| 3 | `ORDR_NUM` | VARCHAR | 0 (0.0%) | distinct=19,930,695; top: (skipped — high cardinality) |
| 4 | `ORDR_LINE_NUM` | DECIMAL(38,10) | 0 (0.0%) | min=0.0010000000, max=931.0000000000, mean=9.9708, median=5.0000000000 |
| 5 | `ORDR_QTY` | DECIMAL(38,10) | 0 (0.0%) | min=-441600.0000000000, max=6170700.0000000000, mean=3.9579, median=1.0000000000 |
| 6 | `UNIT_SLS_AMT` | DECIMAL(38,10) | 0 (0.0%) | min=0E-10, max=1540000.0000000000, mean=75.3165, median=22.1800000000 |
| 7 | `DIM_ORDR_DT_ID` | DECIMAL(38,0) | 0 (0.0%) | min=20240101, max=20260101, mean=20245669.4348, median=20250102 |
| 8 | `ORDR_MTHD_DSC` | VARCHAR | 0 (0.0%) | distinct=12; top: `SUPPLY MANAGER` (66,508,760), `EDI` (18,016,853), `CUSTOMER SERVICE` (10,822,302), `B2B` (9,607,223), `N/A` (3,698,821) |
| 9 | `ORDR_SRC_DSC` | VARCHAR | 0 (0.0%) | distinct=6; top: `ONLINE` (67,170,262), `CONNECTIVITY` (27,624,076), `CUSTOMER SERVICE` (10,952,579), `N/A` (3,669,336), `REP` (1,829,579) |
| 10 | `MCK_FLG` | VARCHAR | 0 (0.0%) | distinct=2; top: `N` (72,317,434), `Y` (39,135,629) |
| 11 | `INV_NUM` | VARCHAR | 0 (0.0%) | distinct=27,561,846; top: (skipped — high cardinality) |
| 12 | `SHIP_QTY` | DECIMAL(38,10) | 0 (0.0%) | min=-441600.0000000000, max=6170700.0000000000, mean=3.9579, median=1.0000000000 |
| 13 | `PRMRY_QTY` | DECIMAL(38,10) | 0 (0.0%) | min=-1000000.0000000000, max=6170700.0000000000, mean=317.4801, median=20.0000000000 |
| 14 | `fiscal_year` | VARCHAR | 0 (0.0%) | distinct=2; top: `FY2526` (55,757,772), `FY2425` (55,695,291) |
| 15 | `order_year` | INTEGER | 0 (0.0%) | min=2024, max=2026, mean=2024.5007, median=2025.0 |
| 16 | `order_month` | INTEGER | 0 (0.0%) | min=1, max=12, mean=6.4726, median=7.0 |
| 17 | `order_day` | INTEGER | 0 (0.0%) | min=1, max=31, mean=15.2753, median=15.0 |

**Sample (first 3 rows):**

|   DIM_CUST_CURR_ID |   DIM_ITEM_E1_CURR_ID |   ORDR_NUM |   ORDR_LINE_NUM |   ORDR_QTY |   UNIT_SLS_AMT |   DIM_ORDR_DT_ID | ORDR_MTHD_DSC    | ORDR_SRC_DSC     | MCK_FLG   |   INV_NUM |   SHIP_QTY |   PRMRY_QTY | fiscal_year   |   order_year |   order_month |   order_day |
|-------------------:|----------------------:|-----------:|----------------:|-----------:|---------------:|-----------------:|:-----------------|:-----------------|:----------|----------:|-----------:|------------:|:--------------|-------------:|--------------:|------------:|
|        1.01876e+07 |                300902 |   13177382 |               1 |         -1 |          72.48 |      2.02401e+07 | CUSTOMER SERVICE | CUSTOMER SERVICE | N         |  28818736 |         -1 |         -12 | FY2425        |         2024 |             1 |          11 |
|        1.58584e+06 |                761464 |   13193901 |               1 |         -2 |         151.47 |      2.02401e+07 | SFDC-TITANIUM    | REP              | Y         |  63953487 |         -2 |        -100 | FY2425        |         2024 |             1 |          11 |
|        5.58468e+06 |                283100 |   13201263 |               1 |         -2 |          20.9  |      2.02401e+07 | CUSTOMER SERVICE | CUSTOMER SERVICE | N         |  28818856 |         -2 |          -2 | FY2425        |         2024 |             1 |          11 |

---

## Layer detail: PRECOMPUTED (serving)

### `precomputed` _(partitioned dataset, 14 shards)_

- **Folder:** `C:\Users\maina\Desktop\Capstone\data_clean\serving\precomputed`
- **Total size:** 434.2 MB
- **Rows:** 26,175,039
- **Columns:** 8

**Columns:**

| # | Column | Type | Nulls | Stats |
|---|---|---|---|---|
| 1 | `DIM_CUST_CURR_ID` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 2 | `DIM_ITEM_E1_CURR_ID` | BIGINT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 3 | `last_order_date` | TIMESTAMP | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 4 | `first_order_date` | TIMESTAMP | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 5 | `n_lines` | INTEGER | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 6 | `total_qty` | FLOAT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 7 | `total_spend` | FLOAT | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |
| 8 | `days_since_last` | INTEGER | ? | err: Invalid Input Error: Failed to read file "C:\Users\maina\Desktop\Capstone\data_c |

**Sample (first 3 rows):**

|   DIM_CUST_CURR_ID |   DIM_ITEM_E1_CURR_ID | last_order_date     | first_order_date    |   n_lines |   total_qty |   total_spend |   days_since_last |
|-------------------:|----------------------:|:--------------------|:--------------------|----------:|------------:|--------------:|------------------:|
|            1990991 |                 34563 | 2024-03-28 00:00:00 | 2024-03-28 00:00:00 |         1 |           6 |         36    |               644 |
|            1383097 |                528558 | 2024-07-11 00:00:00 | 2024-07-11 00:00:00 |         1 |           1 |        291.83 |               539 |
|            6619956 |                126305 | 2024-02-03 00:00:00 | 2024-02-03 00:00:00 |         1 |           1 |        175.86 |               698 |
