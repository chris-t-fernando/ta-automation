

def api_retry(func):
    print("banana1")
    def inner(*args, **kwargs):
        # so you can call func(**kwargs) here in a try...except and put the backoff in one place
        print("banana2")
        return func(*args, **kwargs)
    return inner


@api_retry
def do_something(limit=10.123123, units=50):
    print(f"I am going to sell {units} units at {limit}")

do_something(limit=12312, units=2)