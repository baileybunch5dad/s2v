protoc  -I . --cpp_out=. --grpc_out=. --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` handshake.proto 
protoc  -I . --python_out=. --grpc_out=. --plugin=protoc-gen-grpc=`which grpc_python_plugin` handshake.proto 
 

g++ -std=c++17 -I/home/chris/.local/include  -pthread -c handshake.grpc.pb.cc
g++ -std=c++17 -I/home/chris/.local/include  -pthread -c handshake.pb.cc
g++ -std=c++17 -I/home/chris/.local/include  -pthread -c krm_capp.cpp

g++ krm_capp.o handshake.grpc.pb.o handshake.pb.o -o krm_capp \
/home/chris/.local/lib64/libabsl_flags_parse.a  \
/home/chris/.local/lib/libgrpc++_reflection.a  \
/home/chris/.local/lib/libgrpc++.a  \
/home/chris/.local/lib64/libprotobuf.a  \
/home/chris/.local/lib/libgrpc.a  \
/home/chris/.local/lib/libupb_json_lib.a  \
/home/chris/.local/lib/libupb_textformat_lib.a  \
/home/chris/.local/lib/libupb_mini_descriptor_lib.a  \
/home/chris/.local/lib/libupb_wire_lib.a  \
/home/chris/.local/lib/libutf8_range_lib.a  \
/home/chris/.local/lib/libupb_message_lib.a  \
/home/chris/.local/lib/libupb_base_lib.a  \
/home/chris/.local/lib/libupb_mem_lib.a  \
/home/chris/.local/lib/libre2.a  \
/home/chris/.local/lib/libz.a  \
/home/chris/.local/lib/libcares.a  \
/home/chris/.local/lib/libgpr.a  \
/home/chris/.local/lib/libssl.a  \
/home/chris/.local/lib/libcrypto.a  \
/home/chris/.local/lib/libaddress_sorting.a  \
-ldl -lm -lrt  \
/home/chris/.local/lib64/libabsl_log_internal_check_op.a  \
-pthread  \
/home/chris/.local/lib64/libabsl_leak_check.a  \
/home/chris/.local/lib64/libabsl_die_if_null.a  \
/home/chris/.local/lib64/libabsl_log_internal_conditions.a  \
/home/chris/.local/lib64/libabsl_log_internal_message.a  \
/home/chris/.local/lib64/libabsl_log_internal_nullguard.a  \
/home/chris/.local/lib64/libabsl_examine_stack.a  \
/home/chris/.local/lib64/libabsl_log_internal_format.a  \
/home/chris/.local/lib64/libabsl_log_internal_proto.a  \
/home/chris/.local/lib64/libabsl_log_internal_log_sink_set.a  \
/home/chris/.local/lib64/libabsl_log_sink.a  \
/home/chris/.local/lib64/libabsl_log_entry.a  \
/home/chris/.local/lib64/libabsl_log_initialize.a  \
/home/chris/.local/lib64/libabsl_log_internal_globals.a  \
/home/chris/.local/lib64/libabsl_log_globals.a  \
/home/chris/.local/lib64/libabsl_vlog_config_internal.a  \
/home/chris/.local/lib64/libabsl_log_internal_fnmatch.a  \
/home/chris/.local/lib64/libabsl_random_distributions.a  \
/home/chris/.local/lib64/libabsl_random_seed_sequences.a  \
/home/chris/.local/lib64/libabsl_random_internal_pool_urbg.a  \
/home/chris/.local/lib64/libabsl_random_internal_randen.a  \
/home/chris/.local/lib64/libabsl_random_internal_randen_hwaes.a  \
/home/chris/.local/lib64/libabsl_random_internal_randen_hwaes_impl.a  \
/home/chris/.local/lib64/libabsl_random_internal_randen_slow.a  \
/home/chris/.local/lib64/libabsl_random_internal_platform.a  \
/home/chris/.local/lib64/libabsl_random_internal_seed_material.a  \
/home/chris/.local/lib64/libabsl_random_seed_gen_exception.a  \
/home/chris/.local/lib64/libabsl_statusor.a  \
/home/chris/.local/lib64/libabsl_status.a  \
/home/chris/.local/lib64/libabsl_strerror.a  \
/home/chris/.local/lib64/libutf8_validity.a  \
/home/chris/.local/lib64/libabsl_flags_usage.a  \
/home/chris/.local/lib64/libabsl_flags_usage_internal.a  \
/home/chris/.local/lib64/libabsl_flags_internal.a  \
/home/chris/.local/lib64/libabsl_flags_marshalling.a  \
/home/chris/.local/lib64/libabsl_flags_reflection.a  \
/home/chris/.local/lib64/libabsl_flags_config.a  \
/home/chris/.local/lib64/libabsl_cord.a  \
/home/chris/.local/lib64/libabsl_cordz_info.a  \
/home/chris/.local/lib64/libabsl_cord_internal.a  \
/home/chris/.local/lib64/libabsl_cordz_functions.a  \
/home/chris/.local/lib64/libabsl_cordz_handle.a  \
/home/chris/.local/lib64/libabsl_crc_cord_state.a  \
/home/chris/.local/lib64/libabsl_crc32c.a  \
/home/chris/.local/lib64/libabsl_str_format_internal.a  \
/home/chris/.local/lib64/libabsl_crc_internal.a  \
/home/chris/.local/lib64/libabsl_crc_cpu_detect.a  \
/home/chris/.local/lib64/libabsl_raw_hash_set.a  \
/home/chris/.local/lib64/libabsl_hash.a  \
/home/chris/.local/lib64/libabsl_bad_variant_access.a  \
/home/chris/.local/lib64/libabsl_city.a  \
/home/chris/.local/lib64/libabsl_low_level_hash.a  \
/home/chris/.local/lib64/libabsl_hashtablez_sampler.a  \
/home/chris/.local/lib64/libabsl_exponential_biased.a  \
/home/chris/.local/lib64/libabsl_flags_private_handle_accessor.a  \
/home/chris/.local/lib64/libabsl_flags_commandlineflag.a  \
/home/chris/.local/lib64/libabsl_bad_optional_access.a  \
/home/chris/.local/lib64/libabsl_flags_commandlineflag_internal.a  \
/home/chris/.local/lib64/libabsl_flags_program_name.a  \
/home/chris/.local/lib64/libabsl_synchronization.a  \
/home/chris/.local/lib64/libabsl_graphcycles_internal.a  \
/home/chris/.local/lib64/libabsl_kernel_timeout_internal.a  \
/home/chris/.local/lib64/libabsl_time.a  \
/home/chris/.local/lib64/libabsl_civil_time.a  \
/home/chris/.local/lib64/libabsl_time_zone.a  \
/home/chris/.local/lib64/libabsl_stacktrace.a  \
/home/chris/.local/lib64/libabsl_symbolize.a  \
/home/chris/.local/lib64/libabsl_strings.a  \
/home/chris/.local/lib64/libabsl_strings_internal.a  \
/home/chris/.local/lib64/libabsl_string_view.a  \
/home/chris/.local/lib64/libabsl_int128.a  \
/home/chris/.local/lib64/libabsl_throw_delegate.a  \
/home/chris/.local/lib64/libabsl_malloc_internal.a  \
/home/chris/.local/lib64/libabsl_base.a  \
-lpthread  \
/home/chris/.local/lib64/libabsl_spinlock_wait.a  \
-lrt /home/chris/.local/lib64/libabsl_debugging_internal.a  \
/home/chris/.local/lib64/libabsl_raw_logging_internal.a  \
/home/chris/.local/lib64/libabsl_log_severity.a  \
/home/chris/.local/lib64/libabsl_demangle_internal.a  \
/home/chris/.local/lib64/libabsl_demangle_rust.a  \
/home/chris/.local/lib64/libabsl_decode_rust_punycode.a  \
/home/chris/.local/lib64/libabsl_utf8_for_code_point.a \
