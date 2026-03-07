if(NOT DEFINED CASE_SOURCE)
  message(FATAL_ERROR "CASE_SOURCE is required")
endif()

if(NOT DEFINED BINARY_DIR)
  message(FATAL_ERROR "BINARY_DIR is required")
endif()

if(NOT DEFINED CXX_COMPILER)
  message(FATAL_ERROR "CXX_COMPILER is required")
endif()

if(NOT DEFINED CXX_COMPILER_ID)
  message(FATAL_ERROR "CXX_COMPILER_ID is required")
endif()

if(NOT DEFINED CXX_STANDARD)
  message(FATAL_ERROR "CXX_STANDARD is required")
endif()

if(NOT DEFINED DPE_INCLUDE_DIR)
  message(FATAL_ERROR "DPE_INCLUDE_DIR is required")
endif()

file(MAKE_DIRECTORY "${BINARY_DIR}")

if(CXX_COMPILER_ID STREQUAL "MSVC")
  set(COMPILE_COMMAND
      "${CXX_COMPILER}"
      "/std:c++${CXX_STANDARD}"
      "/I${DPE_INCLUDE_DIR}"
      "/EHsc"
      "/c"
      "${CASE_SOURCE}")
else()
  set(COMPILE_COMMAND
      "${CXX_COMPILER}"
      "-std=c++${CXX_STANDARD}"
      "-I${DPE_INCLUDE_DIR}"
      "-fsyntax-only"
      "${CASE_SOURCE}")
endif()

execute_process(
  COMMAND ${COMPILE_COMMAND}
  WORKING_DIRECTORY "${BINARY_DIR}"
  RESULT_VARIABLE CASE_RESULT
  OUTPUT_VARIABLE CASE_STDOUT
  ERROR_VARIABLE CASE_STDERR)

file(WRITE "${BINARY_DIR}/stdout.log" "${CASE_STDOUT}")
file(WRITE "${BINARY_DIR}/stderr.log" "${CASE_STDERR}")

if(NOT CASE_RESULT MATCHES "^-?[0-9]+$")
  message(
    FATAL_ERROR
      "Failed to execute compiler for ${CASE_SOURCE}: ${CASE_RESULT}\n${CASE_STDERR}")
endif()

if(CASE_RESULT EQUAL 0)
  message(
    FATAL_ERROR
      "Expected compilation failure for ${CASE_SOURCE}, but compilation succeeded.")
endif()
