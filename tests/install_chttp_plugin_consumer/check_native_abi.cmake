cmake_minimum_required(VERSION 3.20)

if(DEFINED CHTTP_TEST_MODULE)
  string(REPLACE "__CHTTP_LIST__" ";" CHTTP_TEST_ALLOWED_ROOTS
         "${CHTTP_TEST_ALLOWED_ROOTS}")
  string(REPLACE "__CHTTP_LIST__" ";" CHTTP_TEST_SEARCH_DIRS
         "${CHTTP_TEST_SEARCH_DIRS}")
  string(REPLACE "__CHTTP_LIST__" ";" CHTTP_TEST_SYSTEM_ROOTS
         "${CHTTP_TEST_SYSTEM_ROOTS}")
  foreach(required_var IN ITEMS
          CHTTP_TEST_ALLOWED_ROOTS CHTTP_TEST_SEARCH_DIRS CHTTP_TEST_CONFIG)
    if(NOT DEFINED ${required_var} OR "${${required_var}}" STREQUAL "")
      message(FATAL_ERROR "Missing required variable: ${required_var}")
    endif()
  endforeach()
  if(DEFINED CHTTP_EXPECT_FAILURE_PATTERN AND
     NOT DEFINED CHTTP_VALIDATE_ONLY)
    string(REPLACE ";" "__CHTTP_LIST__" closure_allowed_roots_arg
           "${CHTTP_TEST_ALLOWED_ROOTS}")
    string(REPLACE ";" "__CHTTP_LIST__" closure_search_dirs_arg
           "${CHTTP_TEST_SEARCH_DIRS}")
    string(REPLACE ";" "__CHTTP_LIST__" closure_system_roots_arg
           "${CHTTP_TEST_SYSTEM_ROOTS}")
    execute_process(
      COMMAND "${CMAKE_COMMAND}"
              "-DCHTTP_TEST_MODULE=${CHTTP_TEST_MODULE}"
              "-DCHTTP_TEST_ALLOWED_ROOTS=${closure_allowed_roots_arg}"
              "-DCHTTP_TEST_SEARCH_DIRS=${closure_search_dirs_arg}"
              "-DCHTTP_TEST_SYSTEM_ROOTS=${closure_system_roots_arg}"
              "-DCHTTP_TEST_CONFIG=${CHTTP_TEST_CONFIG}"
              -DCHTTP_VALIDATE_ONLY=ON
              -P "${CMAKE_CURRENT_LIST_FILE}"
      RESULT_VARIABLE expected_failure_result
      OUTPUT_VARIABLE expected_failure_output
      ERROR_VARIABLE expected_failure_error)
    string(CONCAT expected_failure_diagnostic
           "${expected_failure_output}" "\n${expected_failure_error}")
    if(expected_failure_result EQUAL 0)
      message(FATAL_ERROR "CHTTP closure validation unexpectedly succeeded")
    endif()
    if(NOT expected_failure_diagnostic MATCHES "${CHTTP_EXPECT_FAILURE_PATTERN}")
      message(FATAL_ERROR
              "CHTTP closure validation failed without '${CHTTP_EXPECT_FAILURE_PATTERN}'\n${expected_failure_diagnostic}")
    endif()
    return()
  endif()
  if(NOT EXISTS "${CHTTP_TEST_MODULE}")
    message(FATAL_ERROR "CHTTP closure root does not exist: ${CHTTP_TEST_MODULE}")
  endif()
  if(WIN32)
    list(PREPEND CHTTP_TEST_SYSTEM_ROOTS
         "$ENV{WINDIR}/System32" "$ENV{WINDIR}/SysWOW64")
    list(REMOVE_DUPLICATES CHTTP_TEST_SYSTEM_ROOTS)
    set(windows_system_dependency_files)
    foreach(system_root IN LISTS CHTTP_TEST_SYSTEM_ROOTS)
      file(GLOB system_root_dependency_files "${system_root}/*.dll")
      list(APPEND windows_system_dependency_files
           ${system_root_dependency_files})
    endforeach()
    if(CHTTP_TEST_CONFIG STREQUAL "Release")
      set(windows_forbidden_runtime_regex
          ".*[/\\\\](vcruntime[0-9]*d|msvcp[0-9]*d|ucrtbased)\\.dll$")
    else()
      set(windows_forbidden_runtime_regex
          ".*[/\\\\](vcruntime[0-9]*|msvcp[0-9]*|ucrtbase)\\.dll$")
    endif()
  endif()
  file(GET_RUNTIME_DEPENDENCIES
       LIBRARIES "${CHTTP_TEST_MODULE}"
       DIRECTORIES ${CHTTP_TEST_SEARCH_DIRS}
       RESOLVED_DEPENDENCIES_VAR closure_resolved
       UNRESOLVED_DEPENDENCIES_VAR closure_unresolved
       CONFLICTING_DEPENDENCIES_PREFIX closure_conflicts
       PRE_EXCLUDE_REGEXES "^(api-ms-|ext-ms-)"
       POST_INCLUDE_REGEXES ".*[/\\\\]salts_chttp.*\\.dll$"
                            "${windows_forbidden_runtime_regex}"
       POST_EXCLUDE_FILES ${windows_system_dependency_files})
  if(closure_unresolved)
    message(FATAL_ERROR
            "CHTTP closure has unresolved dependencies: ${closure_unresolved}")
  endif()
  if(closure_conflicts_FILENAMES)
    message(FATAL_ERROR
            "CHTTP closure has conflicting dependencies: ${closure_conflicts_FILENAMES}")
  endif()
  foreach(resolved_dependency IN LISTS closure_resolved)
    cmake_path(GET resolved_dependency FILENAME resolved_name)
    string(TOLOWER "${resolved_name}" resolved_name_lower)
    if(resolved_name_lower MATCHES "^salts_chttp.*\\.dll$")
      message(FATAL_ERROR
              "CHTTP recursive closure contains a legacy salts_chttp DLL: ${resolved_dependency}")
    endif()
    if(CHTTP_TEST_CONFIG STREQUAL "Release" AND
       resolved_name_lower MATCHES
       "^(vcruntime[0-9]*d|msvcp[0-9]*d|ucrtbased)\\.dll$")
      message(FATAL_ERROR
              "Release CHTTP recursive closure contains a Debug CRT: ${resolved_dependency}")
    endif()
    if(CHTTP_TEST_CONFIG STREQUAL "Debug" AND
       resolved_name_lower MATCHES
       "^(vcruntime[0-9]*|msvcp[0-9]*|ucrtbase)\\.dll$")
      message(FATAL_ERROR
              "Debug CHTTP recursive closure contains a Release CRT: ${resolved_dependency}")
    endif()
    set(resolved_is_allowed FALSE)
    set(resolved_is_system_path FALSE)
    foreach(system_root IN LISTS CHTTP_TEST_SYSTEM_ROOTS)
      if(IS_DIRECTORY "${system_root}")
        file(REAL_PATH "${system_root}" system_root_real)
        file(REAL_PATH "${resolved_dependency}" resolved_dependency_real)
        cmake_path(IS_PREFIX system_root_real "${resolved_dependency_real}"
                   NORMALIZE resolved_is_within_system_root)
        if(resolved_is_within_system_root)
          set(resolved_is_system_path TRUE)
        endif()
      endif()
    endforeach()
    foreach(allowed_root IN LISTS CHTTP_TEST_ALLOWED_ROOTS)
      if(IS_DIRECTORY "${allowed_root}")
        file(REAL_PATH "${allowed_root}" allowed_root_real)
        file(REAL_PATH "${resolved_dependency}" resolved_dependency_real)
        cmake_path(IS_PREFIX allowed_root_real "${resolved_dependency_real}"
                   NORMALIZE resolved_is_within_root)
        if(resolved_is_within_root)
          set(resolved_is_allowed TRUE)
        endif()
      endif()
    endforeach()
    if(NOT resolved_is_allowed AND NOT resolved_is_system_path)
      message(FATAL_ERROR
              "CHTTP non-system dependency resolved outside the active profile roots: ${resolved_dependency}")
    endif()
  endforeach()
  if(WIN32)
    find_program(CHTTP_DUMPBIN_EXECUTABLE dumpbin REQUIRED)
    set(CHTTP_POWERSHELL_EXECUTABLE
        "$ENV{WINDIR}/System32/WindowsPowerShell/v1.0/powershell.exe")
    if(NOT EXISTS "${CHTTP_POWERSHELL_EXECUTABLE}")
      message(FATAL_ERROR
              "CHTTP closure validation requires the built-in Windows PowerShell host")
    endif()
    set(chttp_application_modules "${CHTTP_TEST_MODULE}" ${closure_resolved})
    set(selected_system_dependencies)
    foreach(chttp_application_module IN LISTS chttp_application_modules)
      execute_process(
        COMMAND "${CHTTP_DUMPBIN_EXECUTABLE}" /nologo /dependents
                "${chttp_application_module}"
        RESULT_VARIABLE dumpbin_result
        OUTPUT_VARIABLE dumpbin_output
        ERROR_VARIABLE dumpbin_error)
      if(NOT dumpbin_result EQUAL 0)
        message(FATAL_ERROR
                "Unable to inspect CHTTP closure module '${chttp_application_module}': ${dumpbin_error}")
      endif()
      string(REGEX MATCHALL
             "[A-Za-z0-9_.+-]+\\.[Dd][Ll][Ll]" direct_dependency_names
             "${dumpbin_output}")
      cmake_path(GET chttp_application_module PARENT_PATH application_module_dir)
      foreach(direct_dependency_name IN LISTS direct_dependency_names)
        if(EXISTS "${application_module_dir}/${direct_dependency_name}")
          continue()
        endif()
        set(selected_system_dependency)
        foreach(system_root IN LISTS CHTTP_TEST_SYSTEM_ROOTS)
          if(EXISTS "${system_root}/${direct_dependency_name}")
            file(REAL_PATH "${system_root}/${direct_dependency_name}"
                 selected_system_dependency)
            break()
          endif()
        endforeach()
        if(selected_system_dependency)
          list(APPEND selected_system_dependencies
               "${selected_system_dependency}")
        endif()
      endforeach()
    endforeach()
    list(REMOVE_DUPLICATES selected_system_dependencies)
    foreach(selected_system_dependency IN LISTS selected_system_dependencies)
      cmake_path(GET selected_system_dependency FILENAME
                 selected_system_dependency_name)
      string(TOLOWER "${selected_system_dependency_name}"
             selected_system_dependency_name_lower)
      # Windows PowerShell parses -Command arguments again, including path spaces.
      string(REPLACE "'" "''" selected_system_dependency_argument
             "${selected_system_dependency}")
      execute_process(
        COMMAND "${CHTTP_POWERSHELL_EXECUTABLE}" -NoProfile -NonInteractive
                -Command
                "& { param([string]$Path) $ErrorActionPreference = 'Stop'; Import-Module (Join-Path $PSHOME 'Modules/Microsoft.PowerShell.Security/Microsoft.PowerShell.Security.psd1'); Add-Type -TypeDefinition 'using System; using System.Runtime.InteropServices; public static class ChttpWrpIdentity { [DllImport(\"sfc.dll\", CharSet=CharSet.Unicode)] public static extern bool SfcIsFileProtected(IntPtr rpc, string path); }'; $Signature = Get-AuthenticodeSignature -LiteralPath $Path; $Product = (Get-Item -LiteralPath $Path).VersionInfo.ProductName; Write-Output ('Status=' + $Signature.Status); if ($Signature.SignerCertificate.Subject -ceq 'CN=Microsoft Windows, O=Microsoft Corporation, L=Redmond, S=Washington, C=US') { Write-Output 'Publisher=MicrosoftWindows' } elseif ($Signature.SignerCertificate.Subject -ceq 'CN=Microsoft Windows Software Compatibility Publisher, O=Microsoft Corporation, L=Redmond, S=Washington, C=US') { Write-Output 'Publisher=MicrosoftWindowsCompatibility' } elseif ($Signature.SignerCertificate.Subject -ceq 'CN=Microsoft Corporation, O=Microsoft Corporation, L=Redmond, S=Washington, C=US') { Write-Output 'Publisher=MicrosoftCorporation' } else { Write-Output 'Publisher=Untrusted' }; if ([ChttpWrpIdentity]::SfcIsFileProtected([IntPtr]::Zero, $Path)) { Write-Output 'Identity=WindowsOS' } elseif (($Product -ceq 'Microsoft® Windows® Operating System' -or $Product -ceq 'Microsoft® Visual Studio®') -and ([IO.Path]::GetFileName($Path) -match '^(vcruntime[0-9]*d?|msvcp[0-9]*d?|ucrtbased?)\\.dll$')) { Write-Output 'Identity=MicrosoftRuntime' } else { Write-Output 'Identity=NonOS' } }"
                "'${selected_system_dependency_argument}'"
        RESULT_VARIABLE signature_result
        OUTPUT_VARIABLE signature_output
        ERROR_VARIABLE signature_error)
      string(STRIP "${signature_output}" signature_output)
      string(REPLACE "\r\n" ";" signature_identity "${signature_output}")
      string(REPLACE "\n" ";" signature_identity "${signature_identity}")
      set(required_system_identity "WindowsOS")
      if(selected_system_dependency_name_lower MATCHES
         "^(vcruntime[0-9]*d?|msvcp[0-9]*d?|ucrtbased?)\\.dll$")
        set(required_system_identity "(WindowsOS|MicrosoftRuntime)")
      endif()
      if(NOT signature_result EQUAL 0 OR signature_error OR
         NOT signature_identity MATCHES
             "^Status=Valid;Publisher=(MicrosoftWindows|MicrosoftWindowsCompatibility|MicrosoftCorporation);Identity=${required_system_identity}$")
        message(FATAL_ERROR
                "CHTTP system-path dependency lacks the required Microsoft Windows signature: ${selected_system_dependency}\n${signature_output}\n${signature_error}")
      endif()
    endforeach()
  endif()
  return()
endif()

if(DEFINED CHTTP_EXPECT_FAILURE_PATTERN AND
   NOT DEFINED CHTTP_VALIDATE_ONLY)
  execute_process(
    COMMAND "${CMAKE_COMMAND}"
            "-DCHTTP_TEST_LAYER=${CHTTP_TEST_LAYER}"
            "-DCHTTP_TEST_DEPENDENTS=${CHTTP_TEST_DEPENDENTS}"
            -DCHTTP_VALIDATE_ONLY=ON
            -P "${CMAKE_CURRENT_LIST_FILE}"
    RESULT_VARIABLE expected_failure_result
    OUTPUT_VARIABLE expected_failure_output
    ERROR_VARIABLE expected_failure_error)
  string(CONCAT expected_failure_diagnostic
         "${expected_failure_output}" "\n${expected_failure_error}")
  if(expected_failure_result EQUAL 0)
    message(FATAL_ERROR "CHTTP dependency validation unexpectedly succeeded")
  endif()
  if(NOT expected_failure_diagnostic MATCHES "${CHTTP_EXPECT_FAILURE_PATTERN}")
    message(FATAL_ERROR
            "CHTTP dependency validation failed without '${CHTTP_EXPECT_FAILURE_PATTERN}'\n${expected_failure_diagnostic}")
  endif()
  return()
endif()

foreach(required_var IN ITEMS CHTTP_TEST_LAYER CHTTP_TEST_DEPENDENTS)
  if(NOT DEFINED ${required_var} OR "${${required_var}}" STREQUAL "")
    message(FATAL_ERROR "Missing required variable: ${required_var}")
  endif()
endforeach()

string(TOLOWER "${CHTTP_TEST_DEPENDENTS}" chttp_dependents)
if(chttp_dependents MATCHES "salts_chttp[^ \t\r\n]*\\.dll")
  message(FATAL_ERROR
          "CHTTP dependency closure must not contain a legacy salts_chttp DLL")
endif()

if(CHTTP_TEST_LAYER STREQUAL "adapter")
  string(REGEX MATCHALL "(^|[ \t\r\n])chttp_[^ \t\r\n]*dll([ \t\r\n]|$)"
         all_native_imports "${chttp_dependents}")
  foreach(native_import IN LISTS all_native_imports)
    string(STRIP "${native_import}" native_import)
    if(NOT native_import STREQUAL "chttp_client.dll" AND
       NOT native_import STREQUAL "chttp_server.dll")
      message(FATAL_ERROR "CHTTP adapter has an unknown native import: ${native_import}")
    endif()
  endforeach()
  foreach(native_dll_regex IN ITEMS "chttp_client\\.dll" "chttp_server\\.dll")
    string(REGEX MATCHALL "(^|[ \t\r\n])${native_dll_regex}([ \t\r\n]|$)"
           native_imports "${chttp_dependents}")
    list(LENGTH native_imports native_import_count)
    if(NOT native_import_count EQUAL 1)
      string(REPLACE "\\." "." native_dll "${native_dll_regex}")
      message(FATAL_ERROR
              "CHTTP adapter requires exactly one ${native_dll} import")
    endif()
  endforeach()
elseif(CHTTP_TEST_LAYER STREQUAL "provider")
  string(REGEX MATCHALL
         "(^|[ \t\r\n])tf_chttp_adapter\\.dll([ \t\r\n]|$)"
         adapter_imports "${chttp_dependents}")
  list(LENGTH adapter_imports adapter_import_count)
  if(NOT adapter_import_count EQUAL 1)
    message(FATAL_ERROR
            "CHTTP provider requires exactly one tf_chttp_adapter.dll import")
  endif()
  if(chttp_dependents MATCHES "turbo_flow\\.dll")
    message(FATAL_ERROR "CHTTP provider must not depend on turbo_flow.dll")
  endif()
elseif(CHTTP_TEST_LAYER STREQUAL "gateway")
  if(chttp_dependents MATCHES
     "tf_chttp_adapter\\.dll|chttp_client\\.dll|chttp_server\\.dll|turbo_flow\\.dll")
    message(FATAL_ERROR "Gateway must not link a concrete HTTP implementation")
  endif()
else()
  message(FATAL_ERROR "Unknown CHTTP dependency layer: ${CHTTP_TEST_LAYER}")
endif()
