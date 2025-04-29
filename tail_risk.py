import pandas as pd
import numpy as np
import scipy.stats as stats
from scipy import optimize


def gpd_logLike(parms, x, freqs, tol = 1e-6):
   # Extract the parameters 
   xi, sigma = parms
   # Make sure the frequency array is defined
   if freqs is None:
      freqs = np.ones(len(x))
   # Total number of items in the tail (x array)
   k = np.sum(freqs)
   if abs(xi) < tol:
      # Special case when xi = 0 the GPD simplifies to the Exponential Distribution
      loglike = -k * np.log(sigma) - np.sum(x * freqs)/sigma
   else:
      # Check the support domain
      if np.any(1 + xi * x / sigma < 0):
         loglike = -np.inf
      else:
         loglike = -k * np.log(sigma) - (1 + 1.0/xi)*np.sum(np.log(1 + xi * x / sigma) * freqs)
   # print(f"{xi=}   {sigma=}  -->  {loglike=}")
   # Return the Negative Log-Likelihood (since we are minimizing)
   return -loglike

def nonparametric_tail(data, probs, cum_probs = None, p = 0.9, right_side = True):

   # Make sure cumulative probabilities are defined
   if cum_probs is None:
      cum_probs = np.cumsum(probs)
   # Find the index of the quantile
   var_idx = np.min(np.where(cum_probs >= p))
   # Compute VaR
   VaR = data[var_idx]

   # Find the indices of the tail
   if right_side:
      tail_idx = np.where(data >= VaR)
   else:
      tail_idx = np.where(data <= VaR)
   tail_probs = probs[tail_idx]
   # Adjust the probability of the first element of the tail 
   # to account only for the portion that falls within the lower extreme of the integral
   tail_probs[0] = cum_probs[tail_idx][0] - p
   # Compute ES: Expected value of the tail
   ES = np.sum(data[tail_idx] * tail_probs)/np.sum(tail_probs)

   return VaR, ES

# Function to compute VaR and ES
def tail_risk(data, freqs, alpha = 0.95, right_side = True, dist = None, df = None, trsh_pct = 0.9):
   # Make sure alpha is an array
   if not isinstance(alpha, (list, np.ndarray)):
      alpha = [alpha]

   if dist not in [None, "Normal", "T", "GPD"]:
      raise ValueError(f"Invalid value '{dist}' for parameter `dist`. Valid values: None, 'Normal', 'T', 'GPD'.")

   # Multiplier used to sort data accordingly (right side -> 1, left side -> -1)
   side_factor = np.sign(2*right_side - 1)
   # Sort the data and frequencies
   sorted_indices = np.argsort(data * side_factor)
   sorted_data = data[sorted_indices]
   sorted_freqs = freqs[sorted_indices]
   # Compute the probabilities
   N = np.sum(sorted_freqs)
   sorted_probs = sorted_freqs / N
   # Compute the Weighted mean
   mean = np.sum(sorted_data * sorted_probs)
   # Compute the Weighted Standard Deviation
   sigma = np.sqrt(np.sum((sorted_data - mean)**2 * sorted_probs))

   # Cumulative distribution function
   cum_probs = np.cumsum(sorted_probs)

   # Initialize VaR and ES
   VaR = pd.Series(index = alpha, name = "VaR")
   ES = pd.Series(index = alpha, name = "ES")
   # Loop through all 
   for p in alpha:
      # Determine which method must be used
      match(dist):
         case "Normal":
            # p-Quantile on a standardized Normal Distribution
            q = stats.norm.ppf(p)
            # Transform the quantile to account for the mean and standard deviation 
            VaR[p] = mean + side_factor * sigma * q
            ES[p] = mean + side_factor * sigma * stats.norm.pdf(q)/(1-p)
         case "T":
            # Compute probability weighted kurtosis
            kurt = np.sum((sorted_data - mean)**4 * sorted_probs)/sigma**4
            # Set the Degrees of Freedom for the T-Student Distribution (if not provided as input)
            df = df or max(4, kurt)
            # p-Quantile on a standardized T-Student Distribution.
            q =  stats.t.ppf(p, df)
            K = stats.t.pdf(q, df) * (df + q**2) / ((df - 1)* (1-p))
            # Multiply sigma by sqrt((df-2)/df) so that q represents a random variable with unitary variance
            scale = sigma * np.sqrt((df-2)/df)
            # Transform the quantile to account for the mean and standard deviation 
            VaR[p] = mean + side_factor * scale * q
            ES[p] = mean + side_factor * scale * K
         case "GPD":

            # Determine the threshold for the Peak-Over-Threshold method
            trsh, _ = nonparametric_tail(sorted_data
                                          , sorted_probs
                                          , cum_probs = cum_probs
                                          , p = trsh_pct
                                          , right_side = right_side
                                          )
            # Determine the tail indices
            if right_side:
               tail_idx = np.where(sorted_data >= trsh)
            else:
               tail_idx = np.where(sorted_data <= trsh)

            # Extract the tail data            
            tail_data = sorted_data[tail_idx]
            # Extract the associated frequencies. Need to cast to Double to avoud overflow during optimization
            tail_freqs = sorted_freqs[tail_idx].astype(np.double)
            tail_sign = 1
            if np.any(tail_data < 0):
               if np.all(tail_data < 0):
                  # Change the sign of the tail. Log-Likelihood requires positive values
                  tail_sign = -1
               else:
                  raise ValueError("The tail portion of the data includes both positive and negative values. Increase the parameter trsh_pct to ensure the tail contains only values with the same sign.")
            # Compute the excesses as a right-side tail
            u = trsh * tail_sign
            excesses = tail_sign*(tail_data - trsh)

            # Initialize the parameters for fitting the GPD
            x0 = [0.1, 1]
            # Fit the distribution (Maximum Likelihood Estimation)
            xi, sigma = optimize.fmin(gpd_logLike, x0 = x0, args = (excesses, tail_freqs), disp = 0)

            # For the case when shape is nearly zero, we use the limit (exponential distribution) form:
            if abs(xi) > 1e-6:
               VaR_gpd_excess = u + (((1-p)/trsh_pct)**(-xi) - 1) * sigma/xi
            else:
               VaR_gpd_excess = u + sigma * np.log((1-p)/trsh_pct)

            ES_gpd_excess = (VaR_gpd_excess + sigma - xi*u)/(1-xi)

            # Revert the sign to reflect the original left tail
            VaR[p] = tail_sign * VaR_gpd_excess
            ES[p] = tail_sign * ES_gpd_excess
         case _:
            VaR[p], ES[p] = nonparametric_tail(sorted_data
                                               , sorted_probs
                                               , cum_probs = cum_probs
                                               , p = p
                                               , right_side = right_side
                                               )

   # Collect result
   res = pd.DataFrame(dict(VaR = VaR.values, ES = ES.values), index = alpha)
   if dist:
      res.index.name = f"Prob({dist})"
   else:
      res.index.name = "Prob"

   return res