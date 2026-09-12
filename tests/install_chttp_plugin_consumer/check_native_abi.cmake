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
    # VC's numeric suffixes put d last; named MSVCP satellites put d before _.
    set(windows_release_crt "(vcruntime[0-9]+(_1|_threads)?|msvcp[0-9]+(_1|_2|_atomic_wait|_codecvt_ids)?|ucrtbase)")
    set(windows_debug_crt "(vcruntime[0-9]+(_1|_threads)?d|msvcp[0-9]+(_[12]d|d(_atomic_wait|_codecvt_ids)?)|ucrtbased)")
    set(windows_crt_regex "^(${windows_release_crt}|${windows_debug_crt})\\.dll$")
    find_program(CHTTP_DUMPBIN_EXECUTABLE dumpbin REQUIRED NO_CACHE)
    find_program(chttp_host_compiler cl.exe PATHS ENV PATH NO_DEFAULT_PATH REQUIRED NO_CACHE)
    if(NOT IS_DIRECTORY "$ENV{VCToolsInstallDir}")
      message(FATAL_ERROR "CHTTP closure requires the active VsDevCmd VCToolsInstallDir")
    endif()
    file(REAL_PATH "$ENV{VCToolsInstallDir}" chttp_tools_root)
    file(REAL_PATH "${chttp_host_compiler}" chttp_host_compiler)
    cmake_path(GET chttp_host_compiler PARENT_PATH chttp_compiler_dir)
    file(REAL_PATH "${CHTTP_DUMPBIN_EXECUTABLE}" CHTTP_DUMPBIN_EXECUTABLE)
    cmake_path(GET CHTTP_DUMPBIN_EXECUTABLE PARENT_PATH chttp_dumpbin_dir)
    cmake_path(IS_PREFIX chttp_tools_root "${chttp_compiler_dir}" NORMALIZE chttp_compiler_owned)
    if(NOT chttp_compiler_owned OR NOT chttp_compiler_dir STREQUAL chttp_dumpbin_dir)
      message(FATAL_ERROR "CHTTP closure compiler/dumpbin must belong to the active VsDevCmd toolchain")
    endif()
    file(GLOB chttp_compiler_runtimes "${chttp_compiler_dir}/clang_rt.asan_dynamic-*.dll")
    list(LENGTH chttp_compiler_runtimes chttp_compiler_runtime_count)
    if(chttp_compiler_runtime_count GREATER 1)
      message(FATAL_ERROR "CHTTP closure has ambiguous current compiler ASan runtimes: ${chttp_compiler_dir}")
    endif()
    set(chttp_compiler_runtime "")
    if(chttp_compiler_runtime_count EQUAL 1)
      list(GET chttp_compiler_runtimes 0 chttp_compiler_runtime)
      file(REAL_PATH "${chttp_compiler_runtime}" chttp_compiler_runtime)
      cmake_path(GET chttp_compiler_runtime PARENT_PATH chttp_runtime_dir)
      if(NOT chttp_runtime_dir STREQUAL chttp_compiler_dir)
        message(FATAL_ERROR "CHTTP ASan runtime escaped the active compiler directory: ${chttp_compiler_runtime}")
      endif()
    endif()
    # GET_RUNTIME_DEPENDENCIES does not search PATH on Windows. This is a search
    # entry only: no other file in this directory acquires runtime provenance.
    list(APPEND CHTTP_TEST_SEARCH_DIRS "${chttp_compiler_dir}")
    list(PREPEND CHTTP_TEST_SYSTEM_ROOTS
         "$ENV{WINDIR}/System32" "$ENV{WINDIR}/SysWOW64")
    list(REMOVE_DUPLICATES CHTTP_TEST_SYSTEM_ROOTS)
    # CRTs beside the compiler are platform runtimes too, but only these exact
    # basename-qualified files may stop recursion, and every selected one is signed.
    set(chttp_compiler_crt_files)
    file(GLOB chttp_compiler_dlls "${chttp_compiler_dir}/*.dll")
    foreach(chttp_compiler_dll IN LISTS chttp_compiler_dlls)
      cmake_path(GET chttp_compiler_dll FILENAME chttp_compiler_dll_name)
      string(TOLOWER "${chttp_compiler_dll_name}" chttp_compiler_dll_name)
      if(chttp_compiler_dll_name MATCHES "${windows_crt_regex}")
        file(REAL_PATH "${chttp_compiler_dll}" chttp_compiler_dll)
        cmake_path(GET chttp_compiler_dll PARENT_PATH chttp_runtime_dir)
        if(NOT chttp_runtime_dir STREQUAL chttp_compiler_dir)
          message(FATAL_ERROR "CHTTP CRT escaped the active compiler directory: ${chttp_compiler_dll}")
        endif()
        list(APPEND chttp_compiler_crt_files "${chttp_compiler_dll}")
      endif()
    endforeach()
    set(windows_system_dependency_files ${chttp_compiler_crt_files})
    foreach(system_root IN LISTS CHTTP_TEST_SYSTEM_ROOTS)
      file(GLOB system_root_dependency_files "${system_root}/*.dll")
      list(APPEND windows_system_dependency_files
           ${system_root_dependency_files})
    endforeach()
  endif()
  file(GET_RUNTIME_DEPENDENCIES
       LIBRARIES "${CHTTP_TEST_MODULE}"
       DIRECTORIES ${CHTTP_TEST_SEARCH_DIRS}
       RESOLVED_DEPENDENCIES_VAR closure_resolved
       UNRESOLVED_DEPENDENCIES_VAR closure_unresolved
       CONFLICTING_DEPENDENCIES_PREFIX closure_conflicts
       PRE_EXCLUDE_REGEXES "^(api-ms-|ext-ms-)"
       POST_INCLUDE_REGEXES ".*[/\\\\]salts_chttp.*\\.dll$"
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
    if(resolved_name_lower MATCHES "^clang_rt\\.asan.*\\.dll$")
      file(REAL_PATH "${resolved_dependency}" resolved_dependency_real)
      if(NOT resolved_dependency_real STREQUAL chttp_compiler_runtime)
        message(FATAL_ERROR "CHTTP ASan runtime resolved outside the active compiler: ${resolved_dependency}")
      endif()
      cmake_path(GET chttp_compiler_runtime FILENAME chttp_compiler_runtime_name)
      find_file(chttp_path_runtime "${chttp_compiler_runtime_name}"
                PATHS ENV PATH NO_DEFAULT_PATH REQUIRED NO_CACHE)
      file(REAL_PATH "${chttp_path_runtime}" chttp_path_runtime)
      if(NOT chttp_path_runtime STREQUAL chttp_compiler_runtime)
        message(FATAL_ERROR "CHTTP ASan runtime PATH provenance does not match the active compiler: ${chttp_path_runtime}")
      endif()
      set(resolved_is_allowed TRUE)
    endif()
    if(NOT resolved_is_allowed AND NOT resolved_is_system_path)
      message(FATAL_ERROR
              "CHTTP non-system dependency resolved outside the active profile roots: ${resolved_dependency}")
    endif()
  endforeach()
  if(WIN32)
    set(CHTTP_POWERSHELL_EXECUTABLE
        "$ENV{WINDIR}/System32/WindowsPowerShell/v1.0/powershell.exe")
    if(NOT EXISTS "${CHTTP_POWERSHELL_EXECUTABLE}")
      message(FATAL_ERROR
              "CHTTP closure validation requires the built-in Windows PowerShell host")
    endif()
    set(chttp_application_modules "${CHTTP_TEST_MODULE}" ${closure_resolved})
    set(selected_system_dependencies)
    foreach(chttp_application_module IN LISTS chttp_application_modules)
      file(REAL_PATH "${chttp_application_module}" chttp_application_module_real)
      set(chttp_module_config "${CHTTP_TEST_CONFIG}")
      if(chttp_application_module_real STREQUAL chttp_compiler_runtime)
        # VS 17.7+ shares one ASan DLL across /MD and /MDd. Its own CRT is Release.
        # The exact file still passes Authenticode below; application edges do not
        # inherit this compiler-runtime configuration.
        set(chttp_module_config Release)
        list(APPEND selected_system_dependencies "${chttp_compiler_runtime}")
      endif()
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
        string(TOLOWER "${direct_dependency_name}" direct_dependency_name_lower)
        if(chttp_module_config STREQUAL "Release" AND
           direct_dependency_name_lower MATCHES "^${windows_debug_crt}\\.dll$")
          message(FATAL_ERROR "Release CHTTP recursive closure contains a Debug CRT: ${chttp_application_module} -> ${direct_dependency_name}")
        elseif(chttp_module_config STREQUAL "Debug" AND
               direct_dependency_name_lower MATCHES "^${windows_release_crt}\\.dll$")
          message(FATAL_ERROR "Debug CHTTP recursive closure contains a Release CRT: ${chttp_application_module} -> ${direct_dependency_name}")
        endif()
        if(EXISTS "${application_module_dir}/${direct_dependency_name}")
          file(REAL_PATH "${application_module_dir}/${direct_dependency_name}" direct_dependency_real)
          if(direct_dependency_real IN_LIST chttp_compiler_crt_files)
            list(APPEND selected_system_dependencies "${direct_dependency_real}")
          endif()
          continue()
        endif()
        set(selected_system_dependency)
        foreach(system_root IN LISTS CHTTP_TEST_SYSTEM_ROOTS CHTTP_TEST_SEARCH_DIRS)
          if(EXISTS "${system_root}/${direct_dependency_name}")
            file(REAL_PATH "${system_root}/${direct_dependency_name}"
                 selected_system_dependency)
            if(NOT system_root IN_LIST CHTTP_TEST_SYSTEM_ROOTS AND
               NOT selected_system_dependency IN_LIST chttp_compiler_crt_files)
              set(selected_system_dependency)
            endif()
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
      string(REPLACE "'" "''" chttp_compiler_runtime_argument "${chttp_compiler_runtime}")
      execute_process(
        COMMAND "${CHTTP_POWERSHELL_EXECUTABLE}" -NoProfile -NonInteractive
                -Command
                "& { param([string]$Path, [string]$CrtPattern, [string]$CompilerRuntime) $ErrorActionPreference = 'Stop'; Import-Module (Join-Path $PSHOME 'Modules/Microsoft.PowerShell.Security/Microsoft.PowerShell.Security.psd1'); Add-Type -TypeDefinition 'using System; using System.Runtime.InteropServices; public static class ChttpWrpIdentity { [DllImport(\"sfc.dll\", CharSet=CharSet.Unicode)] public static extern bool SfcIsFileProtected(IntPtr rpc, string path); }'; $Signature = Get-AuthenticodeSignature -LiteralPath $Path; $Product = (Get-Item -LiteralPath $Path).VersionInfo.ProductName; Write-Output ('Status=' + $Signature.Status); if ($Signature.SignerCertificate.Subject -ceq 'CN=Microsoft Windows, O=Microsoft Corporation, L=Redmond, S=Washington, C=US') { Write-Output 'Publisher=MicrosoftWindows' } elseif ($Signature.SignerCertificate.Subject -ceq 'CN=Microsoft Windows Software Compatibility Publisher, O=Microsoft Corporation, L=Redmond, S=Washington, C=US') { Write-Output 'Publisher=MicrosoftWindowsCompatibility' } elseif ($Signature.SignerCertificate.Subject -ceq 'CN=Microsoft Corporation, O=Microsoft Corporation, L=Redmond, S=Washington, C=US') { Write-Output 'Publisher=MicrosoftCorporation' } else { Write-Output 'Publisher=Untrusted' }; if ($Path -ceq $CompilerRuntime) { Write-Output 'Identity=CompilerRuntime' } elseif ([ChttpWrpIdentity]::SfcIsFileProtected([IntPtr]::Zero, $Path)) { Write-Output 'Identity=WindowsOS' } elseif (($Product -ceq 'Microsoft® Windows® Operating System' -or $Product -ceq 'Microsoft® Visual Studio®') -and ([IO.Path]::GetFileName($Path) -match $CrtPattern)) { Write-Output 'Identity=MicrosoftRuntime' } else { Write-Output 'Identity=NonOS' } }"
                "'${selected_system_dependency_argument}'"
                "'${windows_crt_regex}'" "'${chttp_compiler_runtime_argument}'"
        RESULT_VARIABLE signature_result
        OUTPUT_VARIABLE signature_output
        ERROR_VARIABLE signature_error)
      string(STRIP "${signature_output}" signature_output)
      string(REPLACE "\r\n" ";" signature_identity "${signature_output}")
      string(REPLACE "\n" ";" signature_identity "${signature_identity}")
      set(required_system_identity "WindowsOS")
      set(required_publisher "(MicrosoftWindows|MicrosoftWindowsCompatibility|MicrosoftCorporation)")
      if(selected_system_dependency STREQUAL chttp_compiler_runtime)
        set(required_system_identity "CompilerRuntime")
        set(required_publisher "MicrosoftCorporation")
      elseif(selected_system_dependency_name_lower MATCHES "${windows_crt_regex}")
        set(required_system_identity "(WindowsOS|MicrosoftRuntime)")
      endif()
      if(NOT signature_result EQUAL 0 OR signature_error OR
         NOT signature_identity MATCHES
             "^Status=Valid;Publisher=${required_publisher};Identity=${required_system_identity}$")
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
