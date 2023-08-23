" """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
" "  Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights
" "  Reserved.
" "  SPDX-License-Identifier: MIT-0
" """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

CLASS zcl_aws1_dyn_actions DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.
  PROTECTED SECTION.
private section.

  methods CREATE_TABLE
    importing
      value(IV_TABLE_NAME) type /AWS1/DYNTABLENAME
    returning
      value(OO_RESULT) type ref to /AWS1/CL_DYNCREATETABLEOUTPUT .
  methods DESCRIBE_TABLE
    importing
      value(IV_TABLE_NAME) type /AWS1/DYNTABLENAME
    returning
      value(OO_RESULT) type ref to /AWS1/CL_DYNDESCRTABLEOUTPUT .
  methods DELETE_TABLE
    importing
      value(IV_TABLE_NAME) type /AWS1/DYNTABLENAME .
  methods LIST_TABLES
    returning
      value(OO_RESULT) type ref to /AWS1/CL_DYNLISTTABLESOUTPUT .
  methods PUT_ITEM
    importing
      value(IV_TABLE_NAME) type /AWS1/DYNTABLENAME
      value(IV_ITEM) type /AWS1/CL_DYNATTRIBUTEVALUE=>TT_PUTITEMINPUTATTRIBUTEMAP .
  methods GET_ITEM
    importing
      value(IV_TABLE_NAME) type /AWS1/DYNTABLENAME
      !IV_KEY type /AWS1/CL_DYNATTRIBUTEVALUE=>TT_KEY
    returning
      value(OO_ITEM) type ref to /AWS1/CL_DYNGETITEMOUTPUT .
  methods UPDATE_ITEM
    importing
      value(IV_TABLE_NAME) type /AWS1/DYNTABLENAME
      value(IT_ITEM_KEY) type /AWS1/CL_DYNATTRIBUTEVALUE=>TT_KEY
      value(IT_ATTRIBUTE_UPDATES) type /AWS1/CL_DYNATTRVALUEUPDATE=>TT_ATTRIBUTEUPDATES
    returning
      value(OO_OUTPUT) type ref to /AWS1/CL_DYNUPDATEITEMOUTPUT .
  methods DELETE_ITEM
    importing
      value(IV_TABLE_NAME) type /AWS1/DYNTABLENAME
      value(IT_KEY_INPUT) type /AWS1/CL_DYNATTRIBUTEVALUE=>TT_KEY .
  methods QUERY_TABLE
    importing
      value(IV_TABLE_NAME) type /AWS1/DYNTABLENAME
      value(IV_KEYCONDITIONS) type /AWS1/CL_DYNCONDITION=>TT_KEYCONDITIONS
    returning
      value(OO_RESULT) type ref to /AWS1/CL_DYNQUERYOUTPUT .
  methods SCAN_ITEMS
    importing
      value(IV_TABLE_NAME) type /AWS1/DYNTABLENAME
    returning
      value(OO_SCAN_RESULT) type ref to /AWS1/CL_DYNSCANOUTPUT .
ENDCLASS.



CLASS ZCL_AWS1_DYN_ACTIONS IMPLEMENTATION.


  METHOD create_table.
    CONSTANTS cv_pfl TYPE /aws1/rt_profile_id VALUE 'ZCODE_DEMO'.

    DATA(lo_session) = /aws1/cl_rt_session_aws=>create( cv_pfl ).
    DATA(lo_dyn) = /aws1/cl_dyn_factory=>create( lo_session ).

    " snippet-start:[dyn.abapv1.create_table]
    TRY.
        DATA(lt_keyschema) = VALUE /aws1/cl_dynkeyschemaelement=>tt_keyschema(
          ( NEW /aws1/cl_dynkeyschemaelement( iv_attributename = 'year'
                                              iv_keytype = 'HASH' ) )
          ( NEW /aws1/cl_dynkeyschemaelement( iv_attributename = 'title'
                                              iv_keytype = 'RANGE' ) ) ).
        DATA(lt_attributedefinitions) = VALUE /aws1/cl_dynattributedefn=>tt_attributedefinitions(
          ( NEW /aws1/cl_dynattributedefn( iv_attributename = 'year'
                                           iv_attributetype = 'N' ) )
          ( NEW /aws1/cl_dynattributedefn( iv_attributename = 'title'
                                           iv_attributetype = 'S' ) ) ).

        " Adjust read/write capacities as desired.
        DATA(lo_dynprovthroughput)  = NEW /aws1/cl_dynprovthroughput(
          iv_readcapacityunits = 5
          iv_writecapacityunits = 5 ).
        oo_result = lo_dyn->createtable(
          it_keyschema = lt_keyschema
          iv_tablename = iv_table_name
          it_attributedefinitions = lt_attributedefinitions
          io_provisionedthroughput = lo_dynprovthroughput ).
        " Table creation can take some time. Wait till table exists before returning.
        lo_dyn->get_waiter( )->tableexists(
          iv_max_wait_time = 200
          iv_tablename     = iv_table_name ).
        MESSAGE 'DynamoDB Table' && iv_table_name && 'created.' TYPE 'I'.
      " This exception can happen if the table already exists
      CATCH /aws1/cx_dynresourceinuseex INTO DATA(lo_resourceinuseex).
        DATA(lv_error) = |"{ lo_resourceinuseex->av_err_code }" - { lo_resourceinuseex->av_err_msg }|.
        MESSAGE lv_error TYPE 'E'.
    ENDTRY.
    " snippet-end:[dyn.abapv1.create_table]
  ENDMETHOD.


  METHOD delete_item.
    CONSTANTS cv_pfl TYPE /aws1/rt_profile_id VALUE 'ZCODE_DEMO'.

    DATA(lo_session) = /aws1/cl_rt_session_aws=>create( cv_pfl ).
    DATA(lo_dyn) = /aws1/cl_dyn_factory=>create( lo_session ).

  " snippet-start:[dyn.abapv1.delete_item]
    TRY.
        DATA(lo_resp) = lo_dyn->deleteitem(
          iv_tablename                = iv_table_name
          it_key                      = it_key_input ).
        MESSAGE 'Deleted one item.' TYPE 'I'.
      CATCH /aws1/cx_dyncondalcheckfaile00.
        MESSAGE 'A condition specified in the operation could not be evaluated.' TYPE 'E'.
      CATCH /aws1/cx_dynresourcenotfoundex.
        MESSAGE 'The table or index does not exist' TYPE 'E'.
      CATCH /aws1/cx_dyntransactconflictex.
        MESSAGE 'Another transaction is using the item' TYPE 'E'.
    ENDTRY.
  " snippet-end:[dyn.abapv1.delete_item]

  ENDMETHOD.


  METHOD delete_table.
    CONSTANTS cv_pfl TYPE /aws1/rt_profile_id VALUE 'ZCODE_DEMO'.

    DATA(lo_session) = /aws1/cl_rt_session_aws=>create( cv_pfl ).
    DATA(lo_dyn) = /aws1/cl_dyn_factory=>create( lo_session ).

  " snippet-start:[dyn.abapv1.delete_table]
    TRY.
        lo_dyn->deletetable( iv_tablename = iv_table_name ).
        " Wait till the table is actually deleted.
        lo_dyn->get_waiter( )->tablenotexists(
          iv_max_wait_time = 200
          iv_tablename     = iv_table_name ).
        MESSAGE 'Table ' && iv_table_name && ' deleted.' TYPE 'I'.
      CATCH /aws1/cx_dynresourcenotfoundex.
        MESSAGE 'The table ' && iv_table_name && ' does not exist' TYPE 'E'.
      CATCH /aws1/cx_dynresourceinuseex.
        MESSAGE 'The table cannot be deleted since it is in use' TYPE 'E'.
    ENDTRY.
  " snippet-end:[dyn.abapv1.delete_table]
  ENDMETHOD.


  METHOD describe_table.
    CONSTANTS cv_pfl TYPE /aws1/rt_profile_id VALUE 'ZCODE_DEMO'.

    DATA(lo_session) = /aws1/cl_rt_session_aws=>create( cv_pfl ).
    DATA(lo_dyn) = /aws1/cl_dyn_factory=>create( lo_session ).

  " snippet-start:[dyn.abapv1.describe_table]
    TRY.
        oo_result = lo_dyn->describetable( iv_tablename = iv_table_name ).
        DATA(lv_tablename) = oo_result->get_table( )->ask_tablename( ).
        DATA(lv_tablearn) = oo_result->get_table( )->ask_tablearn( ).
        DATA(lv_tablestatus) = oo_result->get_table( )->ask_tablestatus( ).
        DATA(lv_itemcount) = oo_result->get_table( )->ask_itemcount( ).
        MESSAGE 'The table name is ' && lv_tablename
            && '. The table ARN is ' && lv_tablearn
            && '. The tablestatus is ' && lv_tablestatus
            && '. Item count is ' && lv_itemcount TYPE 'I'.
      CATCH /aws1/cx_dynresourcenotfoundex.
        MESSAGE 'The table ' && lv_tablename && ' does not exist' TYPE 'E'.
    ENDTRY.
    " snippet-end:[dyn.abapv1.describe_table]

  ENDMETHOD.


  METHOD get_item.
    CONSTANTS cv_pfl TYPE /aws1/rt_profile_id VALUE 'ZCODE_DEMO'.

    DATA(lo_session) = /aws1/cl_rt_session_aws=>create( cv_pfl ).
    DATA(lo_dyn) = /aws1/cl_dyn_factory=>create( lo_session ).

  " snippet-start:[dyn.abapv1.get_item]
    TRY.
        oo_item = lo_dyn->getitem(
          iv_tablename                = iv_table_name
          it_key                      = iv_key ).
        " TYPE REF TO ZCL_AWS1_dyn_GET_ITEM_OUTPUT
        DATA(ot_attr) = oo_item->get_item( ).
        DATA(lo_title) = ot_attr[ key = 'title' ]-value.
        DATA(lo_year) = ot_attr[ key = 'year' ]-value.
        DATA(lo_rating) = ot_attr[ key = 'rating' ]-value.
        MESSAGE 'Movie name is: ' && lo_title->get_s( )
          && 'Movie year is: ' && lo_year->get_n( )
          && 'Moving rating is: ' && lo_rating->get_n( ) TYPE 'I'.
      CATCH /aws1/cx_dynresourcenotfoundex.
        MESSAGE 'The table or index does not exist' TYPE 'E'.
    ENDTRY.
  " snippet-end:[dyn.abapv1.get_item]

  ENDMETHOD.


  METHOD list_tables.
    CONSTANTS cv_pfl TYPE /aws1/rt_profile_id VALUE 'ZCODE_DEMO'.

    DATA(lo_session) = /aws1/cl_rt_session_aws=>create( cv_pfl ).
    DATA(lo_dyn) = /aws1/cl_dyn_factory=>create( lo_session ).

  " snippet-start:[dyn.abapv1.list_tables]
    TRY.
        oo_result = lo_dyn->listtables( ).
        " You can loop over the oo_result to get table properties like this
        LOOP AT oo_result->get_tablenames( ) INTO DATA(lo_table_name).
          DATA(lv_tablename) = lo_table_name->get_value( ).
        ENDLOOP.
        DATA(lv_tablecount) = lines( oo_result->get_tablenames( ) ).
        MESSAGE 'Found ' && lv_tablecount && ' tables' TYPE 'I'.
      CATCH /aws1/cx_rt_service_generic INTO DATA(lo_exception).
        DATA(lv_error) = |"{ lo_exception->av_err_code }" - { lo_exception->av_err_msg }|.
        MESSAGE lv_error TYPE 'E'.
    ENDTRY.
    " snippet-end:[dyn.abapv1.list_tables]

  ENDMETHOD.


  METHOD put_item.
    CONSTANTS cv_pfl TYPE /aws1/rt_profile_id VALUE 'ZCODE_DEMO'.

    DATA(lo_session) = /aws1/cl_rt_session_aws=>create( cv_pfl ).
    DATA(lo_dyn) = /aws1/cl_dyn_factory=>create( lo_session ).

  " snippet-start:[dyn.abapv1.put_item]
    TRY.
        DATA(lo_resp) = lo_dyn->putitem(
          iv_tablename = iv_table_name
          it_item      = iv_item ).
        MESSAGE '1 row inserted into DynamoDB Table' && iv_table_name TYPE 'I'.
      CATCH /aws1/cx_dyncondalcheckfaile00.
        MESSAGE 'A condition specified in the operation could not be evaluated.' TYPE 'E'.
      CATCH /aws1/cx_dynresourcenotfoundex.
        MESSAGE 'The table or index does not exist' TYPE 'E'.
      CATCH /aws1/cx_dyntransactconflictex.
        MESSAGE 'Another transaction is using the item' TYPE 'E'.
    ENDTRY.
  " snippet-end:[dyn.abapv1.put_item]

  ENDMETHOD.


  METHOD QUERY_TABLE.
    CONSTANTS cv_pfl TYPE /aws1/rt_profile_id VALUE 'ZCODE_DEMO'.

    DATA(lo_session) = /aws1/cl_rt_session_aws=>create( cv_pfl ).
    DATA(lo_dyn) = /aws1/cl_dyn_factory=>create( lo_session ).

  " snippet-start:[dyn.abapv1.query_table]

    TRY.
        " Query movies .
        oo_result = lo_dyn->query(
          iv_tablename = iv_table_name
          it_keyconditions = iv_keyconditions ).
        DATA(lt_items) = oo_result->get_items( ).
        "You can loop over the results to get item attributes
        LOOP AT lt_items INTO DATA(lo_item).
          DATA(lo_title) = lo_item[ key = 'title' ]-value.
          DATA(lo_year) = lo_item[ key = 'year' ]-value.
        ENDLOOP.
        DATA(lv_count) = oo_result->get_count( ).
        MESSAGE 'Item count is: ' && lv_count TYPE 'I'.
      CATCH /aws1/cx_dynresourcenotfoundex.
        MESSAGE 'The table or index does not exist' TYPE 'E'.
    ENDTRY.
  " snippet-end:[dyn.abapv1.query_items]

  ENDMETHOD.


  METHOD scan_items.
    CONSTANTS cv_pfl TYPE /aws1/rt_profile_id VALUE 'ZCODE_DEMO'.

    DATA(lo_session) = /aws1/cl_rt_session_aws=>create( cv_pfl ).
    DATA(lo_dyn) = /aws1/cl_dyn_factory=>create( lo_session ).

    " snippet-start:[dyn.abapv1.scan_items]
    TRY.
        oo_scan_result = lo_dyn->scan( iv_tablename = iv_table_name ).
        DATA(lt_items) = oo_scan_result->get_items( ).
        LOOP AT lt_items INTO DATA(lo_item).
          " You can loop over to get individual attributes.
          DATA(lo_title) = lo_item[ key = 'title' ]-value.
          DATA(lo_year) = lo_item[ key = 'year' ]-value.
        ENDLOOP.
        DATA(lv_count) = oo_scan_result->get_count( ).
        MESSAGE 'Found ' && lv_count && ' items' TYPE 'I'.
      CATCH /aws1/cx_dynresourcenotfoundex.
        MESSAGE 'The table or index does not exist' TYPE 'E'.
    ENDTRY.
    " snippet-end:[dyn.abapv1.scan_items]

  ENDMETHOD.


  METHOD update_item.
    CONSTANTS cv_pfl TYPE /aws1/rt_profile_id VALUE 'ZCODE_DEMO'.

    DATA(lo_session) = /aws1/cl_rt_session_aws=>create( cv_pfl ).
    DATA(lo_dyn) = /aws1/cl_dyn_factory=>create( lo_session ).

  " snippet-start:[dyn.abapv1.update_item]
    TRY.
        oo_output = lo_dyn->updateitem(
          iv_tablename        = iv_table_name
          it_key              = it_item_key
          it_attributeupdates = it_attribute_updates ).
        MESSAGE '1 item updated in DynamoDB Table' && iv_table_name TYPE 'I'.
      CATCH /aws1/cx_dyncondalcheckfaile00.
        MESSAGE 'A condition specified in the operation could not be evaluated.' TYPE 'E'.
      CATCH /aws1/cx_dynresourcenotfoundex.
        MESSAGE 'The table or index does not exist' TYPE 'E'.
      CATCH /aws1/cx_dyntransactconflictex.
        MESSAGE 'Another transaction is using the item' TYPE 'E'.
    ENDTRY.
  " snippet-end:[dyn.abapv1.update_item]

  ENDMETHOD.
ENDCLASS.
