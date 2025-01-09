# we use Heston model as the volatility model
# setup `n_trader` traders with different funds and gap
# each trader will decide to buy or sell based on the current rate and volatility
# the rate is controlled by PID (and path-dependent on trading).
# %%
using Plots
using Random, Distributions

# %%
# https://en.wikipedia.org/wiki/Heston_model
κ = 2 # rate reverts to θ
θ = 0.25 # long (avg) variance
ξ = 0.5 # volatility of volatility
# we shall have 2κθ > ξ^2
Δt = 1/365/24
ΔW = Normal(0, √Δt)
heston(v) = v + κ*(θ-v)*Δt + ξ*√v*rand(ΔW)

function nested(f, n; init=0)
  x, a = init, [init]
  for i in 1:n-1
    x = f(x)
    push!(a, x)
  end
  a
end

# %%
n = 10000
vol = nested(heston, n; init=θ); plot(vol) |> display
n_trader = 100
funds = rand(Pareto(3), n_trader); funds = funds / sum(funds); histogram(funds)
rate_init = 0.05
gap = 0.25 * rand(LogNormal(0, 0.25), n_trader); histogram(gap) |> display

# %%
state = (r = 0.05, u = 0.0, v=θ, e=0.0, o=(i=0.0,))
decision = fill(0, n_trader)
function next_state(state; u0=0.8, action_rate=0.1, args=(kp=0.005, ki=0.0, kd=0.0))
  rate, vol, other = state.r, heston(state.v), state.o
  action = rand(Uniform(), length(decision))
  decision[vol./(gap.+1).>rate .&& action.<=action_rate] .= 1
  decision[vol.*(gap.+1).<rate .&& action.<=action_rate] .= 0
  u = sum(decision.*funds)
  err = u-u0; err = (err<0)*err/u0 + (err>0)*err/(1-u0)
  # rate *= 0.5 + (u>0.4)*0.3 + (u>0.75)*0.2 + (u>0.85)*0.25 + (u>0.95)*0.75
  # rate *= exp(err)
  other = (;i = other.i + err)
  rate += args.kp * err +  args.ki * other.i + args.kd * (err-state.e)
  (r=rate, u, v=vol, e=err, o=other)
end
states = nested(next_state, n+100; init=state)[101:end]

histogram([s.u for s in states]) |> display
histogram([s.o.i for s in states]) |> display
plot([[s.r for s in states], [s.v for s in states]]) |> display
