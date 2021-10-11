from threading import current_thread
from time import perf_counter


def perf_imports() -> None:
    import builtins

    original_import = builtins.__import__  # type: ignore[misc]
    timer = perf_counter
    # timer = time
    start_time = timer()

    def custom_import(name: str, *args: str, **kwargs: object) -> object:
        import_start_time = timer()
        result = original_import(name, *args, **kwargs)  # type: ignore[misc,arg-type]
        cur_time = timer()
        time_taken = cur_time - import_start_time
        if time_taken >= 0.001:  # and not name.startswith("qualia") and not name.startswith("crypto"):
            print(
                f'{time_taken:04.2f}s after {cur_time - start_time:04.2f}s used to load module {name} on {current_thread().name}')
        return result  # type: ignore[misc]

    builtins.__import__ = custom_import  # type: ignore[assignment]
