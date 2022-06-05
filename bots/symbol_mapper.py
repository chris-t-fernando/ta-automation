class SymbolMapper():
    yf:str
    sw:str
    al:str

    def __init__(self, yf_symbol:str, store=None):
        self.yf = yf_symbol
        self.sw = yf_symbol[:-4]
        self.al = yf_symbol[:-4]

a = SymbolMapper(yf_symbol="TRX-USD")
print(a.yf)
print(a.sw)
print(a.al)