def timed(func):
    def wrapper(*args, **kwargs):
        import time
        start = time.time()
        print(f"Starting {func.__name__}".center(80, "-"))
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            raise e
        finally:
            end = time.time()
            time_elapsed_ms = round((end - start) * 1000, 3)
            print(f"Finished {func.__name__} in {time_elapsed_ms} ms".center(80, "-"), end="\n\n")
    return wrapper
