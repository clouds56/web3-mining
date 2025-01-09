# %%
using Distributions, Random
using Plots

# %%
n=10000
σ=1/√(365*86400/15)

function reduce_nested(f, list; init=0)
  x, a = init, [init]
  for i in list
    x = f(x, i)
    push!(a, x)
  end
  a
end
function step(fee; σ=σ, n=n, init=1.0)
  delta = rand(LogNormal(-σ^2/2, σ), n)
  price = init*cumprod(delta)

  price2=reduce_nested((acc,i)->clamp(acc,i*(1-fee),i*(1+fee)), price; init=init)
  # plot([price, price2]) |> display

  # %%
  sum(abs.(diff(sqrt.(price2))))
end

# %%
fee=[0.0005,0.005,0.01,0.02,0.03,0.04,0.05,0.1,0.2,0.3,0.4,0.5]
result=mean(map(_->step.(fee).*fee,1:1000))
# plot(fee->mean(map(_->step(fee;σ=σ).*fee,1:30)), 0, 0.2) |> display

# %%
σ^2/4

# %%
fee=0.02
mean(map(_->map(i->step(fee;n=i,init=1.0)*fee/i,[1000,2000,5000,10000,20000,50000]), 1:1000))

# %%
map(_-> rand(LogNormal(-σ^2/2, σ), 10000) |> cumprod |> std, 1:100) |> std
