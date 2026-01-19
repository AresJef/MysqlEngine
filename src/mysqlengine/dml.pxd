# cython: language_level=3
from sqlcycli.aio.pool cimport Pool, PoolConnection, PoolSyncConnection
from mysqlengine.element cimport Element

# Clause
cdef class CLAUSE:
    cdef:
        # Index Hints
        list _index_hints
        # Conditions
        list _conditions
        dict _in_conditions
        dict _not_in_conditions
        # Internal
        Py_ssize_t _hashcode
    # Compose
    cpdef str clause(self, str pad=?)
    # Index Hints
    cdef inline bint _add_index_hints(self, INDEX_HINTS index_hints) except -1
    cdef inline str _attach_index_hints(self, str clause, str pad=?)
    # Conditions
    cdef inline str _validate_condition(self, str cond)
    cdef inline list _prepare_conditions(self, str pad=?)

# . Select Clause
cdef class SELECT(CLAUSE):
    cdef:
        list _expressions
        bint _distinct
        bint _high_priority
        bint _straight_join
        bint _sql_buffer_result
    cpdef SELECT setup(self, list expressions, bint distinct=?, bint high_priority=?, bint straight_join=?, bint sql_buffer_result=?)
    cpdef str clause(self, str pad=?)

cdef class FROM(CLAUSE):
    cdef:
        object _table_reference
        list _partition
        str _alias
    cpdef FROM setup(self, object table_reference, list partition, str alias)
    cpdef str clause(self, str pad=?)

cdef class JOIN(CLAUSE):
    cdef:
        str _join_method
        object _table_reference
        list _partition
        list _using_columns
        str _alias
    cpdef JOIN setup(self, str join_method, object table_reference, list partition, list on_conditions, list using_columns, str alias)
    cpdef str clause(self, str pad=?)

cdef class INDEX_HINTS(CLAUSE):
    cdef:
        str _mode
        list _indexes
        str _scope
    cpdef INDEX_HINTS setup(self, str mode, list indexes, str scope)
    cpdef str clause(self, str pad=?)

cdef class WHERE(CLAUSE):
    cpdef WHERE setup(self, list conditions, dict in_conditions, dict not_in_conditions)
    cpdef str clause(self, str pad=?)

cdef class GROUP_BY(CLAUSE):
    cdef:
        list _columns
        bint _with_rollup
    cpdef GROUP_BY setup(self, list columns, bint with_rollup)
    cpdef str clause(self, str pad=?)

cdef class HAVING(CLAUSE):
    cpdef HAVING setup(self, list conditions, dict in_conditions, dict not_in_conditions)
    cpdef str clause(self, str pad=?)

cdef class WINDOW(CLAUSE):
    cdef:
        str _name
        list _options
        bint _primary
    cpdef WINDOW setup(self, str name, list options)
    cpdef str clause(self, str pad=?)

cdef class SET_OPERATION(CLAUSE):
    cdef:
        str _operation
        SelectDML _subquery
        bint _all
    cpdef SET_OPERATION setup(self, str operation, SelectDML subquery, bint all)
    cpdef str clause(self, str pad=?)

cdef class ORDER_BY(CLAUSE):
    cdef:
        list _columns
        bint _with_rollup
    cpdef ORDER_BY setup(self, list columns, bint with_rollup)
    cpdef str clause(self, str pad=?)

cdef class LIMIT(CLAUSE):
    cdef:
        unsigned long long _row_count
        unsigned long long _offset
    cpdef LIMIT setup(self, unsigned long long row_count, unsigned long long offset)
    cpdef str clause(self, str pad=?)

cdef class LOCKING_READS(CLAUSE):
    cdef:
        str _mode
        list _tables
        str _option
    cpdef LOCKING_READS setup(self, str mode, list tables, str option)
    cpdef str clause(self, str pad=?)

cdef class INTO_VARIABLES(CLAUSE):
    cdef:
        list _variables
    cpdef INTO_VARIABLES setup(self, list variables)
    cpdef str clause(self, str pad=?)

# . Insert Clause
cdef class INSERT(CLAUSE):
    cdef:
        str _table
        list _partition
        bint _ignore
        str _priority
        bint _replace_mode
    cpdef INSERT setup(self, str table, list partition, bint ignore, str priority)
    cdef inline bint _set_replace_mode(self, bint replace_mode) except -1
    cpdef str clause(self, str pad=?)

cdef class INSERT_COLUMNS(CLAUSE):
    cdef:
        list _columns
    cpdef INSERT_COLUMNS setup(self, list columns)
    cpdef str clause(self, str pad=?)

cdef class INSERT_VALUES(CLAUSE):
    cdef:
        int _placeholders
        ROW_ALIAS _row_alias
    cpdef INSERT_VALUES setup(self, int placeholders)
    cdef inline bint _add_row_alias(self, ROW_ALIAS row_alias) except -1
    cpdef str clause(self, str pad=?)

cdef class SET(CLAUSE):
    cdef:
        list _assignments
        ROW_ALIAS _row_alias
    cpdef SET setup(self, list assignments)
    cdef inline bint _add_row_alias(self, ROW_ALIAS row_alias) except -1
    cpdef str clause(self, str pad=?)

cdef class ROW_ALIAS(CLAUSE):
    cdef:
        str _row_alias
        list _col_alias
    cpdef ROW_ALIAS setup(self, str row_alias, list col_alias)
    cpdef str clause(self, str pad=?)

cdef class ON_DUPLICATE(CLAUSE):
    cdef:
        list _assignments
    cpdef ON_DUPLICATE setup(self, list assignments)
    cpdef str clause(self, str pad=?)

# . Update Clause
cdef class UPDATE(CLAUSE):
    cdef:
        object _table_reference
        list _partition
        bint _ignore
        bint _low_priority
        str _alias
    cpdef UPDATE setup(self, object table_reference, list partition, bint ignore, bint low_priority, str alias)
    cpdef str clause(self, str pad=?)

# . Delete Clause
cdef class DELETE(CLAUSE):
    cdef:
        object _table_reference
        list _partition
        bint _ignore
        bint _low_priority
        bint _quick
        str _alias
        list _multi_tables
    cpdef DELETE setup(self, object table_reference, list partition, bint ignore, bint low_priority, bint quick, str alias, list multi_tables)
    cpdef bint _has_multi_tables(self) except -1
    cpdef str clause(self, str pad=?)

# . WITH Clause
cdef class WITH(CLAUSE):
    cdef:
        str _name
        object _subquery
        list _columns
        bint _recursive
        bint _primary
    cpdef WITH setup(self, str name, object subquery, list columns, bint recursive)
    cpdef str clause(self, str pad=?)

# DML
cdef class DML:
    cdef:
        # settings
        str _dml
        str _db_name
        Pool _pool
        # clauses
        list _with_clauses
        SELECT _select_clause
        FROM _from_clause
        list _join_clauses
        WHERE _where_clause
        GROUP_BY _group_by_clause
        HAVING _having_clause
        list _window_clauses
        list _set_op_clauses
        ORDER_BY _order_by_clause
        LIMIT _limit_clause
        list _locking_reads_clauses
        INTO_VARIABLES _into_clause
        # connection
        bint _require_conn
        PoolSyncConnection _sync_conn
        PoolConnection _async_conn
        # internal
        int _clause_id
        int _tb_id
        bint _multi_table
        Py_ssize_t _hashcode
    # Connection
    cpdef bint _set_connection(self, PoolSyncConnection sync_conn=?, PoolConnection async_conn=?) except -1
    # SELECT Clause
    cpdef SELECT _gen_select_clause(self, tuple expressions, bint distinct=?, bint high_priority=?, bint straight_join=?, bint sql_buffer_result=?)
    cpdef FROM _gen_from_clause(self, object table, object partition=?, object alias=?)
    cdef inline JOIN _gen_join_clause(self, str join_method, object table, tuple on, object using=?, bint require_condition=?, object partition=?, object alias=?)
    cpdef JOIN _gen_inner_join_clause(self, object table, tuple on, object using=?, object partition=?, object alias=?)
    cpdef JOIN _gen_left_join_clause(self, object table, tuple on, object using=?, object partition=?, object alias=?)
    cpdef JOIN _gen_right_join_clause(self, object table, tuple on, object using=?, object partition=?, object alias=?)
    cpdef JOIN _gen_straight_join_clause(self, object table, tuple on, object using=?, object partition=?, object alias=?)
    cpdef JOIN _gen_cross_join_clause(self, object table, tuple on, object using=?, object partition=?, object alias=?)
    cpdef JOIN _gen_natural_join_clause(self, object table, object join_method=?, object partition=?, object alias=?)
    cdef INDEX_HINTS _gen_index_hints_clause(self, str mode, tuple indexes, object scope=?)
    cpdef INDEX_HINTS _gen_use_index_clause(self, tuple indexes, object scope=?)
    cpdef INDEX_HINTS _gen_ignore_index_clause(self, tuple indexes, object scope=?)
    cpdef INDEX_HINTS _gen_force_index_clause(self, tuple indexes, object scope=?)
    cpdef WHERE _gen_where_clause(self, tuple conds, object in_conds=?, object not_in_conds=?)
    cpdef GROUP_BY _gen_group_by_clause(self, tuple columns, bint with_rollup=?)
    cpdef HAVING _gen_having_clause(self, tuple conds, object in_conds=?, object not_in_conds=?)
    cpdef WINDOW _gen_window_clause(self, object name, object partition_by=?, object order_by=?, object frame_clause=?)
    cpdef SET_OPERATION _gen_union_clause(self, object subquery, bint all=?)
    cpdef SET_OPERATION _gen_intersect_clause(self, object subquery, bint all=?)
    cpdef SET_OPERATION _gen_except_clause(self, object subquery, bint all=?)
    cpdef ORDER_BY _gen_order_by_clause(self, tuple columns, bint with_rollup=?)
    cpdef LIMIT _gen_limit_clause(self, object row_count, object offset=?)
    cdef inline LOCKING_READS _gen_locking_reads_clause(self, str mode, tuple tables, object option=?)
    cpdef LOCKING_READS _gen_for_update_clause(self, tuple tables, object option=?)
    cpdef LOCKING_READS _gen_for_share_clause(self, tuple tables, object option=?)
    cpdef INTO_VARIABLES _gen_into_variables_clause(self, tuple variables)
    # INSERT Clause
    cpdef INSERT _gen_insert_clause(self, object table, object partition=?, bint ignore=?, object priority=?)
    cpdef INSERT_COLUMNS _gen_insert_columns_clause(self, tuple columns)
    cpdef INSERT_VALUES _gen_insert_values_clause(self, int placeholders)
    cpdef SET _gen_set_clause(self, tuple assignments)
    cpdef ROW_ALIAS _gen_row_alias_clause(self, object row_alias, tuple col_alias)
    cpdef ON_DUPLICATE _gen_on_duplicate_clause(self, tuple assignments)
    # UPDATE Clause
    cpdef UPDATE _gen_update_clause(self, object table, object partition=?, bint ignore=?, bint low_priority=?, object alias=?)
    # DELETE Clause
    cpdef DELETE _gen_delete_clause(self, object table, object partition=?, bint ignore=?, bint low_priority=?, bint quick=?, object alias=?, object multi_tables=?)
    # WITH Clause
    cpdef WITH _gen_with_clause(self, object name, object subquery, tuple columns, bint recursive=?)
    # Statement
    cpdef str statement(self, int indent=?)
    cdef inline str _gen_select_statement(self, str pad=?)
    cdef inline str _gen_select_subquery(self, str pad=?)
    # Execute
    cpdef object _Execute(self, object args=?, object cursor=?, bint fetch=?, bint fetch_all=?, bint many=?, object conn=?)
    # Validate
    cdef inline str _validate_element_name(self, str name, str msg)
    cdef inline str _validate_element(self, object element, str msg)
    cdef inline list _validate_elements(self, object elements, str msg)
    cdef inline str _validate_table(self, object table, str msg)
    cdef inline list _validate_tables(self, object tables, str msg)
    cdef inline str _validate_table_alias(self, object alias, str msg)
    cdef inline str _validate_expression(self, object expression, str msg)
    cdef inline list  _validate_expressions(self, object expressions, str msg)
    cdef inline dict _validate_in_conditions(self, object in_conds, str msg)
    cdef inline SelectDML _validate_subquery(self, object subquery, str msg)
    cdef inline str  _validate_indent(self, int indent)
    # Error
    cdef inline bint _raise_error(self, type err_type, str msg, Exception tb_exc=?) except -1
    cdef inline bint _raise_clause_error(self, str msg, Exception tb_exc=?) except -1
    cdef inline bint _raise_argument_error(self, str msg, Exception tb_exc=?) except -1
    cdef inline bint _raise_critical_error(self, str msg, Exception tb_exc=?) except -1
    cdef inline bint _raise_not_implemented_error(self, str method_name) except -1
    cdef inline bint _raise_invalid_table_element_error(self, Element element, Exception tb_exc=?) except -1
    cdef inline bint _raise_clause_already_set_error(self, CLAUSE clause, Exception tb_exc=?) except -1
    cdef inline bint _raise_clause_order_error(self, str clause_name, list preceding_clauses, Exception tb_exc=?) except -1

# . Select
cdef class SelectDML(DML):
    # Clause
    cpdef SelectDML _Select(self, tuple expressions, bint distinct=?, bint high_priority=?, bint straight_join=?, bint sql_buffer_result=?)
    cpdef SelectDML From(self, object table, object partition=?, object alias=?)
    # Statement
    cpdef str statement(self, int indent=?)
    # Execute
    cpdef object Execute(self, object args=?, object cursor=?, bint fetch=?, bint fetch_all=?, object conn=?)
    # Validate
    cdef inline bint _validate_join_clause_order(self) except -1

# . Insert
cdef class InsertDML(DML):
    cdef:
        # clauses
        INSERT _insert_clause
        INSERT_COLUMNS _columns_clause
        INSERT_VALUES _values_clause
        SET _set_clause
        ON_DUPLICATE _on_dup_key_update_clause
        # internal
        int _insert_mode
    # Clause
    cpdef InsertDML Insert(self, object table, object partition=?, bint ignore=?, object priority=?)
    # Statement
    cpdef str statement(self, int indent=?)
    # Execute
    cpdef object Execute(self, object args=?, bint many=?, object conn=?)
    # Validate
    cdef inline bint _validate_join_clause_order(self) except -1

# . Replace
cdef class ReplaceDML(DML):
    cdef:
        # clauses
        INSERT _replace_clause
        INSERT_COLUMNS _columns_clause
        INSERT_VALUES _values_clause
        SET _set_clause
        # internal
        int _insert_mode
    # Clause
    cpdef ReplaceDML Replace(self, object table, object partition=?, bint low_priority=?)
    # Statement
    cpdef str statement(self, int indent=?)
    # Execute
    cpdef object Execute(self, object args=?, bint many=?, object conn=?)
    # Validate
    cdef inline bint _validate_join_clause_order(self) except -1

# . Update
cdef class UpdateDML(DML):
    cdef:
        # clauses
        UPDATE _update_clause
        SET _set_clause
    # Clause
    cpdef UpdateDML Update(self, object table, object partition=?, bint ignore=?, bint low_priority=?, object alias=?)
    # Statement
    cpdef str statement(self, int indent=?)
    # Execute
    cpdef object Execute(self, object args=?, bint many=?, object conn=?)
    # Validate
    cdef inline bint _validate_join_clause_order(self) except -1

# . Delete
cdef class DeleteDML(DML):
    cdef:
        # clauses
        DELETE _delete_clause
    # Clause
    cpdef DeleteDML Delete(self, object table, object partition=?, bint ignore=?, bint low_priority=?, bint quick=?, object alias=?, object multi_tables=?)
    # Statement
    cpdef str statement(self, int indent=?)
    # Execute
    cpdef object Execute(self, object args=?, bint many=?, object conn=?)
    # Validate
    cdef inline bint _validate_join_clause_order(self) except -1

# . With
cdef class WithDML(DML):
    # Clause
    cpdef WithDML _With(self, object name, object subquery, tuple columns, bint recursive=?)
