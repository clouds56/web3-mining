using Distributions
using Plots

σ = 500
_year = 365*86400
_month = 30*86400
_interval = 15
feeRate = 0.003
N = 10000000
ε = log(1+feeRate)

d = Normal(0, σ / sqrt(_year / _interval))
x0 = ones(N)
delta_move(x0) = clamp.(x0.+rand(d, N), -ε, ε)
x0 = reduce((x0, _) -> delta_move(x0), 1:300; init = x0)
histogram(x0[-ε.<x0.<-0.0025]; bins=100)
# sort(x0[x0.>1/(1+feeRate)])[1:10]
