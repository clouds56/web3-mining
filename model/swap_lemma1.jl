# %%
using Plots; gr()
using Distributions
using SpecialFunctions
using SymPy

# %%
module CoreF
  import SymPy
  struct Op
    name::String

    expr_f::Union{SymPy.Sym, Nothing}

    # when you have x = t, you have y = f(t) in the pool
    # f(x; σ=1, T=1) = cdf(Normal(), -σ*√T-quantile(Normal(), x)) # f^-1(y) = f(y)
    expr_y::SymPy.Sym
    expr_dy::SymPy.Sym
    expr_ddy::SymPy.Sym

    g::Function
    p::Function
    od::Function

    function Op(name::String; f::Union{SymPy.Sym, Nothing} = nothing, g::Union{SymPy.Sym, Nothing} = nothing, vars)
      println((f, g))
      t, _, k = vars
      SymPy.@syms t2
      @assert f !== nothing || g !== nothing
      expr_y = if g !== nothing
        println(f, " using g: ", g)
        g
      else
        # from f(t) + f(t2) = 0 to resolve y = g(x)
        y = SymPy.solve(f.subs(t, t2) + f, t2)
        println(f, " resolved: ", y)
        @assert length(y) == 1
        k*y[1] |> SymPy.simplify
      end

      # every dx would exchange -dy, that is to say price of x is -dy/dx = -dy_t
      # d_f(x; σ=1, T=1) = -exp(-σ^2*T/2 - σ*√T*quantile(Normal(), x)) # f'(x)
      # price(y; σ=1, T=1) = exp(-σ^2*T/2 - σ*√T*quantile(Normal(), y)) # p(y) = -d(f^-1(y)), y)
      expr_dy = SymPy.diff(expr_y, t) |> SymPy.simplify # dy/dx = f'(x) = -p_x(x) = -1/p_y(x)
      # every dp would sell -dx, that is to say orderbook of x is -dx/dp = 1/ddy_t
      # orderbook(p; σ=1, T=1) = #
      expr_ddy = SymPy.diff(expr_dy, t) |> SymPy.simplify # f'' = d(dy_t)/dx = -d(p_x)/dx = 1/od_x(x)

      g_ = SymPy.lambdify(expr_y, vars) # y = g(x)
      p_ = SymPy.lambdify(-expr_dy, vars) # p = p(x)
      od_ = SymPy.lambdify(1/expr_ddy, vars) # od = od(x)

      fun_g(x; σ=1., k=1) = g_(x, σ, k)
      fun_p(x; σ=1., k=1) = p_(x, σ, k)
      fun_od(x; σ=1., k=1) = od_(x, σ, k)

      new(name, f, expr_y, expr_dy, expr_ddy, fun_g, fun_p, fun_od)
    end
  end
end

# %%
vars = t,c,k = SymPy.symbols("t,c,k", real=true) # x: BTC, y: USD, t <- x
# quantile(x) + quantile(y) + c == 0
op_erf = CoreF.Op("erf"; f=erfcinv(2*t) - c / SymPy.sqrt(SymPy.Sym(2)) / 2, vars)
# (1-x)^c + (1-y)^c = 1 (c > 1)
op_ellipse = CoreF.Op("ellipse"; f=(1-t)^c-1/SymPy.Sym(2), vars)
# 1/(x+1)^c + 1/(y+1)^c = 1/2^c+1
op_hyperbola = CoreF.Op("hyperbola"; f=1/(1+t)^c - 1/SymPy.Sym(2) - 1/2^(c+1), g=k*(1/(1/2^c+1 - 1/(1+t)^c)^(1/c) - 1), vars)
# (x+1/c)(y+1/c) = (c+1)/c^2
op_xy = CoreF.Op("xy"; f=SymPy.log((c*(t+1/c))/SymPy.sqrt(c+1)), g=k*(1/(1/2^c+1 - 1/(1+t)^c)^(1/c)-1), vars)
# atanh(x*2-1) + atanh(y*2-1) + c == 0
op_atanh = CoreF.Op("atanh"; f=SymPy.atanh(t*2-1)+c/2, vars)

op_fi_g = SymPy.solve(4c*(t+k)+1-4c-1/(t*k)/4, k)[2]
op_fi = CoreF.Op("curve_fi"; g=k*op_fi_g, vars)

# %% for jpy plotting test, using p = F'(y) / F'(x)
# max_price = 1
# testx = 1 - 1/(1/(t/k)^(c/(c-1))+1)^(1/c) # here t represents p
# testx_lambda = SymPy.lambdify(testx, (t,c,k))
# ps = ts .* (max_price * 1.5)
# plot(testx_lambda.(ps, 1.1, max_price), ps)
# testx_dt = SymPy.diff(testx, t)
# testx_dt_lambda = SymPy.lambdify(testx_dt, (t,c,k))
# plot(ps, -testx_dt_lambda.(ps, 1.1, max_price))

# %%
σs = [0.01, 0.05, 0.1, 0.2, 0.5, 0.8, 1, 1.2, 1.5, 2, 100]
max_price = 60

ts = 0:0.0001:1
xs = -10:0.1:10

plot(ts, quantile.(Normal(), ts), title = "quantile")
plot(xs, cdf.(Normal(), xs), title = "cdf")

# %%
function op_plots(op::CoreF.Op, ts, max_price, σs; dir=".")
  subplot = plot(title = "$(op.name): y - x",
    ylims=(0, max_price),
    xlabel = "BTC", ylabel = "USD")
  mkpath("$(dir)/$(op.name)")
  for σ in σs
    plot!(subplot,
      ts,
      op.g.(ts; σ=σ, k=max_price),
      label = "sigma_$σ")
  end
  savefig(subplot, "$(dir)/$(op.name)/y.png")

  subplot = plot(title = "$(op.name): p - x",
    ylims=(0, 5*max_price),
    xlabel = "BTC", ylabel = "price")
  for σ in σs
    plot!(subplot,
      ts,
      op.p.(ts; σ=σ, k=max_price),
      label = "sigma_$σ")
  end
  savefig(subplot, "$(dir)/$(op.name)/price.png")

  # subplot = plot(xlims=(0, max_price*1.5),ylims=(0, 10/max_price),
  #     xlabel = "price", ylabel = "BTC")
  # for σ in σs
  #   plot!(subplot,
  #     p.(ts; σ=σ, k=max_price),
  #     od.(ts; σ=σ, k=max_price),
  #     label = "sigma_$σ")
  # end
  # savefig(subplot, "$(dir)/$(op.name)/price.png")

  subplot = plot(title = "$(op.name): od - p",
    xlims=(0, max_price*2), ylims=(0, 10/max_price),
    xlabel = "price", ylabel = "BTC")
  for σ in σs
    plot!(subplot,
      op.p.(ts; σ=σ, k=max_price),
      op.od.(ts; σ=σ, k=max_price),
      label = "sigma_$σ")
  end
  savefig(subplot, "$(dir)/$(op.name)/orderbook.png")

  subplot = plot(title = "$(op.name): tv - x",
    xlabel = "BTC", ylabel = "value")
  for σ in σs
    plot!(subplot,
      ts,
      op.g.(ts; σ=σ, k=max_price) + ts .* op.p.(ts; σ=σ, k=max_price),
      label = "sigma_$σ")
  end
  savefig(subplot, "$(dir)/$(op.name)/total_value.png")

  subplot = plot(title = "$(op.name): tv - p",
    xlims=(0, max_price*3),
    xlabel = "price", ylabel = "value")
  for σ in σs
    plot!(subplot,
      op.p.(ts; σ=σ, k=max_price),
      op.g.(ts; σ=σ, k=max_price) + ts .* op.p.(ts; σ=σ, k=max_price),
      label = "sigma_$σ")
  end
  savefig(subplot, "$(dir)/$(op.name)/total_value2.png")
end

dir = "model/output"
op_plots(op_erf, ts, max_price, σs; dir)
op_plots(op_ellipse, ts, max_price, exp.(0.6 .* σs); dir)
op_plots(op_hyperbola, ts, max_price, σs .^ 3; dir)
op_plots(op_xy, ts, max_price, σs; dir)
op_plots(op_atanh, ts, max_price, σs; dir)
op_plots(op_fi, 0.01:0.0001:1, max_price, 100 .* σs[1:end-1]; dir)

# %%
# subplot = plot()
# for σ in σs
#   plot!(subplot,
#     p.(ts; σ=σ, k=max_price),
#     od.(ts; σ=σ, k=max_price) .* p.(ts; σ=σ, k=max_price),
#     label = "sigma_$σ", xlims=(0, max_price*3),
#     xlabel = "price", ylabel = "value", yscale=:log10)
# end
# subplot

# %%
max_price = 1

@syms c2
expr_c2 = SymPy.solve(SymPy.solve(op_erf.expr_f, t)[1] - SymPy.solve(op_ellipse.expr_f, t)[1].subs(c, c2), c2)[1] |> SymPy.simplify
f_c2 = SymPy.lambdify(expr_c2, (c,))
plot(0:0.01:10, f_c2.(0:0.01:10))

# %%
σ1s = [0.01, 0.03, 0.06, 0.1, 0.3, 0.5, 0.8, 1, 2, 10]
# σ2s = [0.006, 0.0632, 0.33, 0.47]
# σ2s = (√5-1)/2 .* σ1s
# σ2s = 0.632 .* σ1s
σ2s = f_c2.(σ1s)

op1, op2 = op_erf, op_ellipse
# op_plots(op1, ts, max_price, σ1s; dir="model/output")
# op_plots(op2, ts, max_price, exp.(σ2s); dir="model/output")
subplot = plot(title = "od - p",
  xlims=(0, max_price*2),ylims=(0, 10/max_price),
  xlabel = "price", ylabel = "BTC")
for σ in σ1s
  plot!(subplot,
    op1.p.(ts; σ=σ, k=max_price),
    op1.od.(ts; σ=σ, k=max_price),
    label = "erf_$σ", linestyle=:dash)
end
for σ in σ2s
  plot!(subplot,
    op2.p.(ts; σ=σ, k=max_price),
    op2.od.(ts; σ=σ, k=max_price),
    label = "xy_$(round(σ, digits=5))")
end
subplot

# %%
subplot = plot(title = "y - x")
for σ in σ1s
  plot!(subplot,
    ts,
    op1.g.(ts; σ=σ, k=max_price),
    label = "erf_$σ", linestyle=:dash,
    xlabel = "price", ylabel = "BTC")
end
for σ in σ2s
  plot!(subplot,
    ts,
    op2.g.(ts; σ=σ, k=max_price),
    label = "xy_$(round(σ, digits=5))",
    xlabel = "price", ylabel = "BTC")
end
subplot


# %%
plot(σ1s, σ2s)

# %%
