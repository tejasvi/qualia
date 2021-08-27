from time import perf_counter, time


def perf_imports() -> None:
    import builtins

    original_import = builtins.__import__

    def custom_import(name: str, *args, **kwargs) -> None:
        import_start_time = perf_counter()
        result = original_import(name, *args, **kwargs)
        time_taken = perf_counter() - import_start_time
        if time_taken >= 0.05:
            print(f'{time_taken:04.2f} seconds used to load module {name}')
        return result

    builtins.__import__ = custom_import


# perf_imports()

start_time = time()
