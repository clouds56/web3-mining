# %%
using SymPy
using IterTools
using Plots

# here p > 1, p = (1+r)^t
# x means ST, y means PT
# K is (1+r0)^t
@syms x y t p A r0 K Δx Δy

N_A = 1
N_K = 2
# N_Δx = 3
N_Δy = 10
N_x = 1000
N_y = 1000

# %%
p0 = log(y/x) / A + K

f_price_iter(p) = log((y-Δy)/(x+Δy/p)) / A + K
p_star = f_price_iter(1)
p1′ = f_price_iter(p_star)


# %%
# p_fixed = solve(f_price_iter(p) - p, p)[1]
# p_fixed is not solvable
Nf_price_iter(p) = subs(f_price_iter(p), x=>N_x, y=>N_y, A=>N_A, K=>N_K, Δy=>N_Δy) |> SymPy.N
subs.(
  [p0, p_star, p1′, Iterators.take(iterated(Nf_price_iter, p1′), 6)...],
  x=>N_x, y=>N_y, A=>N_A, K=>N_K, Δy=>N_Δy,
) |> x -> SymPy.N.(x)

# conclusion: p_star < p1′ < ... < p_fixed < p0

# %%
function f_price_step(x0, y0, Δy0; n=1000000)
  dy = Δy0 / n
  x1, y1 = x0, y0
  for _ in 1:n
    p_current = log((y1-dy)/(x1+dy)) / N_A + N_K
    x1 += dy / p_current
    y1 -= dy
  end
  return Δy0 / (x1 - x0)
end
p_s = f_price_step(N_x, N_y, N_Δy)
p_s, Nf_price_iter(p_s)

# conclusion: p0 > p_hat > p_s > f(p_s) > p_fixed

# %%
# assuming p1 == p_fixed
# p_fixed = solve(f_price_iter(p) - p, p)[1]
p_fixed = nth(iterated(Nf_price_iter, 1), 1000)
subs(Δy/solve(log((y-Δy)/(x+Δx)) / A + K - p, Δx)[1], x=>N_x, y=>N_y, A=>N_A, K=>N_K, p=>p_fixed, Δy=>N_Δy)

# %%
# plotting err - Δy
# err = [f_price_step(N_x, N_y, Δy) for Δy in 1:100]
