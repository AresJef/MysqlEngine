# cython: language_level=3
from mysqlengine.element cimport Element, Elements, Logs, Metadata

# Partitioning
cdef class Partitioning(Element):
    cdef:
        # partition
        int _partitioning_flag
        str _partitioning_expression
        # subpartition
        int _subpartitioning_flag
        str _subpartitioning_expression
        # partitions
        Partitions _partitions
    # Configure
    cpdef Partitioning by_hash(self, int partitions, bint linear=?)
    cpdef Partitioning by_key (self, int partitions, bint linear=?)
    # Sync
    cpdef Logs Initialize(self, bint force=?)
    cpdef Logs Create(self)
    cpdef bint Exists(self) except -1
    cpdef Logs Remove(self)
    cpdef PartitioningMetadata ShowMetadata(self)
    cpdef tuple ShowPartitionNames(self)
    cpdef dict ShowPartitionRows(self)
    cpdef tuple ShowSubpartitionNames(self)
    cpdef dict ShowSubpartitionRows(self)
    cpdef Logs SyncFromRemote(self)
    cpdef Logs SyncToRemote(self)
    cpdef bint ExistsPartition(self, object partition)
    cpdef bint EmptyPartition(self, object partition)
    cpdef Logs CoalescePartition(self, int number)
    # Generate SQL
    cpdef str _gen_definition_sql(self)
    cpdef str _gen_create_sql(self)
    cpdef str _gen_exists_sql(self)
    cpdef str _gen_remove_sql(self)
    cpdef str _gen_show_metadata_sql(self)
    cpdef str _gen_show_partition_names_sql(self)
    cpdef str _gen_show_partition_rows_sql(self)
    cpdef str _gen_show_subpartition_names_sql(self)
    cpdef str _gen_show_subpartition_rows_sql(self)
    cpdef str _gen_add_partition_sql(self, tuple partitions)
    cpdef str _gen_exists_partition_sql(self, object partition)
    cpdef str _gen_drop_partition_sql(self, tuple partitions)
    cpdef str _gen_empty_partition_sql(self, object partition)
    cpdef str _gen_reorganize_partition_sql(self, object old_partitions, tuple new_partitions)
    cpdef str _gen_coalesce_partition_sql(self, int number)
    cpdef str _gen_general_partition_sql(self, tuple partitions, str operation)
    # Metadata
    cpdef Logs _sync_from_metadata(self, PartitioningMetadata meta, Logs logs=?)
    cpdef bint _diff_from_metadata(self, PartitioningMetadata meta) except -1
    # Setter
    cpdef bint setup(self, str tb_name, str db_name, object charset, object collate, object pool) except -1
    cpdef bint _set_partitioning_flag(self, int flag) except -1
    cpdef bint _set_partitions_by_instance(self, tuple partitions) except -1
    cpdef bint _set_partitions_by_integer(self, int partitions) except -1
    cpdef bint _reset_subpartitioning(self) except -1
    # Assure Ready
    cdef inline bint _assure_partitioning_flag_ready(self) except -1
    cdef inline bint _assure_partitions_ready(self) except -1
    # Validate
    cdef inline str _validate_partitioning_expression(self, tuple expressions, bint subpartitioning=?)
    cdef inline str _validate_partition(self, object partition)
    cdef inline tuple _validate_partitions(self, object partitions)
    # Copy
    cpdef Partitioning copy(self)

# Partition
cdef class Partition(Element):
    cdef:
        object _values
        str _comment
        int _partitioning_flag
        int _subpartitioning_flag
        Partitions _subpartitions
        bint _is_subpartition
    # Generate SQL
    cpdef str _gen_definition_sql(self)
    # Metadata
    cpdef Logs _sync_from_metadata(self, dict meta, Logs logs=?)
    # Setter
    cpdef bint setup(self, str tb_name, str db_name, object charset, object collate, object pool) except -1
    cpdef bint _set_partitioning_flag(self, int flag) except -1
    cpdef bint _setup_subpartitions(self, int flag, int subpartitions) except -1
    cpdef bint _reset_subpartitions(self) except -1
    cpdef bint _set_as_subpartition(self) except -1
    # Assure Ready
    cdef inline bint _assure_partitioning_flag_ready(self) except -1
    # Validate
    cdef inline tuple _validate_partition_values(self, tuple values)
    # Copy
    cpdef Partition copy(self)

# Partitions
cdef class Partitions(Elements):
    cdef:
        int _partitioning_flag
        int _subpartitioning_flag
        int _subpartition_count
    # Generate SQL
    cpdef str _gen_definition_sql(self, int indent=?)
    # Setter
    cpdef bint setup(self, str tb_name, str db_name, object charset, object collate, object pool) except -1
    cpdef bint _set_partitioning_flag(self, int flag) except -1
    cpdef bint _setup_subpartitions(self, int flag, int subpartitions) except -1
    cpdef bint _reset_subpartitions(self) except -1
    # Assure Ready
    cdef inline bint _assure_partitioning_flag_ready(self) except -1
    # Copy
    cpdef Partitions copy(self)

# Metadata
cdef class PartitioningMetadata(Metadata):
    cdef:
        # Base data
        str _db_name
        str _tb_name
        str _partitioning_method
        str _partitioning_expression
        str _subpartitioning_method
        str _subpartitioning_expression
        list _partitions
        # Additional data
        tuple _partition_names
        int _partition_count
        tuple _subpartition_names
        int _subpartition_count
