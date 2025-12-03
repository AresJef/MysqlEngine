# cython: language_level=3
from mysqlengine.element cimport Element, Elements, Logs, Metadata, Query

# Constraint
cdef class Constraint(Element):
    cdef:
        # Common
        bint _enforced
        # Primary/Unique Key
        tuple _columns
        str _index_type
        str _comment
        bint _visible
        # Foreign Key
        str _reference_table
        tuple _reference_columns
        str _on_delete
        str _on_update
        # Check
        str _expression
    # Sync
    cpdef Logs Initialize(self, bint force=?)
    cpdef Logs Add(self)
    cpdef bint Exists(self) except -1
    cpdef Logs Drop(self)
    cpdef Logs _Alter(
        self, object columns, str index_type, str comment, bool visible, 
        object reference_table, object reference_columns, str on_delete, str on_update,
        object expression, bool enforced,
    )
    cpdef ConstraintMetadata ShowMetadata(self)
    cpdef tuple ShowConstraintSymbols(self)
    cpdef Logs SyncFromRemote(self)
    cpdef Logs SyncToRemote(self)
    # Generate SQL
    cpdef str _gen_definition_sql(self)
    cpdef str _gen_add_sql(self)
    cpdef str _gen_exists_sql(self)
    cpdef str _gen_drop_sql(self)
    cpdef Query _gen_alter_query(
        self, ConstraintMetadata meta,
        object columns, str index_type, str comment, bool visible,
        object reference_table, object reference_columns, str on_delete, str on_update,
        object expression, bool enforced,
    )
    cpdef str _gen_show_metadata_sql(self)
    cpdef str _gen_show_constraint_symbols_sql(self)
    # Metadata
    cpdef Logs _sync_from_metadata(self, ConstraintMetadata meta, Logs logs=?)
    cpdef int _diff_from_metadata(self, ConstraintMetadata meta) except -1
    # Setter
    cpdef bint setup(self, str tb_name, str db_name, object charset, str collate, object pool) except -1
    cpdef bint _set_symbol(self) except -1
    # Copy
    cpdef Constraint copy(self)
    cpdef Constraint _construct(
        self, object columns, str index_type, str comment, bool visible,
        object reference_table, object reference_columns, str on_delete, str on_update,
        object expression, bool enforced,
    )

cdef class UniqueKey(Constraint):
    # Sync
    cpdef Logs SetVisible(self, bint visible)
    # Generate SQL
    cpdef str _gen_set_visible_sql(self, bint visible)

cdef class PrimaryKey(UniqueKey):
    pass

cdef class ForeignKey(Constraint):
    # Validator
    cdef inline str _validate_foreign_key_action(self, object action)

cdef class Check(Constraint):
    # Sync
    cpdef Logs SetEnforced(self, bint enforced)
    # Generate SQL
    cpdef str _gen_set_enforced_sql(self, bint enforced)

# Constraints
cdef class Constraints(Elements):
    # Setter
    cpdef bint setup(self, str tb_name, str db_name, object charset, str collate, object pool) except -1
    # Copy
    cpdef Constraints copy(self)

# Metadata
cdef class ConstraintMetadata(Metadata):
    cdef:
        # Base data
        str _db_name
        str _tb_name
        str _constraint_name
        str _constraint_type
        bint _enforced
        # Additional data
        str _el_type
        # Primary/Unique Key
        str _index_type
        bint _unique
        str _comment
        bint _visible
        tuple _columns
        # Foreign Key
        str _reference_table
        str _unique_constraint
        str _on_delete
        str _on_update
        tuple _reference_columns
        # Check
        str _expression

cdef class UniPriKeyMetadata(ConstraintMetadata):
    pass

cdef class ForeignKeyMetadata(ConstraintMetadata):
    pass

cdef class CheckMetadata(ConstraintMetadata):
    pass
