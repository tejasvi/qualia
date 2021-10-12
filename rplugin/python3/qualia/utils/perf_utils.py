from threading import current_thread
from time import time


def perf_imports() -> None:
    import builtins

    original_import = builtins.__import__
    # timer = perf_counter
    timer = time
    last_time = start_time = timer()

    def custom_import(name: str, *args: str, **kwargs: object) -> object:
        nonlocal last_time
        import_start_time = timer()
        result = original_import(name, *args, **kwargs)  # type: ignore
        cur_time = timer()
        time_taken = cur_time - import_start_time
        if time_taken >= 0.05:  # and not name.startswith("qualia") and not name.startswith("crypto"):
            print(
                f'{time_taken:04.2f}s after {cur_time - start_time:04.2f}s (change: {(cur_time - last_time) * 1000:04.2f}ms) used to load module {name} on {current_thread().name}')
        last_time = cur_time
        return result  # type: ignore

    setattr(builtins, '__import__', custom_import)
