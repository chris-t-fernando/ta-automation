import time

class Banana():
    def __init__(self, symbol, quantity, sell_limit):
        self.symbol = symbol
        self.quantity = quantity
        self.sell_limit = sell_limit


banana_list = []

start_time = time.time()
for i in range(4000000):
    banana_list.append(Banana("aapl", 100, 1235.1235))
print(f"{round(time.time() - start_time,1)}")