from time import perf_counter, time


def perf_imports() -> None:
    import builtins

    original_import = builtins.__import__
    timer = perf_counter
    timer = time

    def custom_import(name: str, *args, **kwargs) -> None:
        import_start_time = timer()
        result = original_import(name, *args, **kwargs)
        time_taken = timer() - import_start_time
        if time_taken >= 0.001:
            print(f'{time_taken:04.2f} seconds used to load module {name}')
        return result

    builtins.__import__ = custom_import
