# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# NO CHECKED-IN PROTOBUF GENCODE
# source: mapreduce.proto
# Protobuf Python Version: 5.29.0
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(
    _runtime_version.Domain.PUBLIC,
    5,
    29,
    0,
    '',
    'mapreduce.proto'
)
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0fmapreduce.proto\x12\tmapreduce\"_\n\nJobRequest\x12\x0e\n\x06job_id\x18\x01 \x01(\t\x12\x12\n\ninput_data\x18\x02 \x03(\t\x12\x14\n\x0cmap_function\x18\x03 \x01(\t\x12\x17\n\x0freduce_function\x18\x04 \x01(\t\"-\n\x0bJobResponse\x12\x0e\n\x06job_id\x18\x01 \x01(\t\x12\x0e\n\x06status\x18\x02 \x01(\t\"C\n\x07MapTask\x12\x0e\n\x06job_id\x18\x01 \x01(\t\x12\x12\n\ndata_chunk\x18\x02 \x01(\t\x12\x14\n\x0cmap_function\x18\x03 \x01(\t\"6\n\tMapResult\x12\x0e\n\x06job_id\x18\x01 \x01(\t\x12\x19\n\x11intermediate_data\x18\x02 \x03(\t\"P\n\nReduceTask\x12\x0e\n\x06job_id\x18\x01 \x01(\t\x12\x19\n\x11intermediate_data\x18\x02 \x03(\t\x12\x17\n\x0freduce_function\x18\x03 \x01(\t\".\n\x0cReduceResult\x12\x0e\n\x06job_id\x18\x01 \x01(\t\x12\x0e\n\x06output\x18\x02 \x01(\t2\xc7\x01\n\x10MapReduceService\x12:\n\tSubmitJob\x12\x15.mapreduce.JobRequest\x1a\x16.mapreduce.JobResponse\x12\x36\n\nProcessMap\x12\x12.mapreduce.MapTask\x1a\x14.mapreduce.MapResult\x12?\n\rProcessReduce\x12\x15.mapreduce.ReduceTask\x1a\x17.mapreduce.ReduceResultb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'mapreduce_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  DESCRIPTOR._loaded_options = None
  _globals['_JOBREQUEST']._serialized_start=30
  _globals['_JOBREQUEST']._serialized_end=125
  _globals['_JOBRESPONSE']._serialized_start=127
  _globals['_JOBRESPONSE']._serialized_end=172
  _globals['_MAPTASK']._serialized_start=174
  _globals['_MAPTASK']._serialized_end=241
  _globals['_MAPRESULT']._serialized_start=243
  _globals['_MAPRESULT']._serialized_end=297
  _globals['_REDUCETASK']._serialized_start=299
  _globals['_REDUCETASK']._serialized_end=379
  _globals['_REDUCERESULT']._serialized_start=381
  _globals['_REDUCERESULT']._serialized_end=427
  _globals['_MAPREDUCESERVICE']._serialized_start=430
  _globals['_MAPREDUCESERVICE']._serialized_end=629
# @@protoc_insertion_point(module_scope)
