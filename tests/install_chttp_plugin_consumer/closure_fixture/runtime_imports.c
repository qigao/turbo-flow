#ifdef _DEBUG
#define VC_ONE fixture_vcruntime140_1d
#define VC_THREADS fixture_vcruntime140_threadsd
#define CPP_ONE fixture_msvcp140_1d
#define CPP_TWO fixture_msvcp140_2d
#define CPP_ATOMIC fixture_msvcp140d_atomic_wait
#define CPP_CODECVT fixture_msvcp140d_codecvt_ids
#else
#define VC_ONE fixture_vcruntime140_1
#define VC_THREADS fixture_vcruntime140_threads
#define CPP_ONE fixture_msvcp140_1
#define CPP_TWO fixture_msvcp140_2
#define CPP_ATOMIC fixture_msvcp140_atomic_wait
#define CPP_CODECVT fixture_msvcp140_codecvt_ids
#endif
__declspec(dllimport) int VC_ONE(void);
__declspec(dllimport) int VC_THREADS(void);
__declspec(dllimport) int CPP_ONE(void);
__declspec(dllimport) int CPP_TWO(void);
__declspec(dllimport) int CPP_ATOMIC(void);
__declspec(dllimport) int CPP_CODECVT(void);

/* Forces real CRT basename imports for inspection; this fixture is never loaded. */
__declspec(dllexport) int fixture_runtime_imports(void)
{
    return VC_ONE() + VC_THREADS() + CPP_ONE() + CPP_TWO() + CPP_ATOMIC() + CPP_CODECVT();
}
