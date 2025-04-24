from typing import Dict
import numpy as np
# from timing import Timer 
import math

class DynamicDist:

   def __init__(self, n_bins: int = 1000, buffer_size: int = 100000): # , timer_enabled: bool = False):
      # self.timer: Timer = Timer(enabled = timer_enabled)
      self.reset(n_bins = n_bins, buffer_size = buffer_size)

   def reset(self, n_bins: int = 1000, buffer_size: int = 100000):
      # Size of the buffer used to hold the initial set of samples
      self.buffer_size: int = buffer_size
      # Number of bins
      self.n_bins: int = n_bins
      # Number of samples processed
      self.n: int = 0
      # Number of duplicate values
      self.n_dups: int = 0
      # Bin Size
      self.bin_size: np.double = np.nan
      # Dictionary to hold all the bins
      self.bins: Dict[np.double, int] = dict()
      # Bin offset
      self.bin_offset: np.double = np.nan

      # Initialize the buffer
      self._buffer: list = [None] * self.buffer_size #np.empty((self.buffer_size), dtype = np.double)


   def compute_hist(self, data, freqs, compute_bins = True):
      # Performance Timer: Start
      # self.timer.start()

      if compute_bins:
         # Map each data point to a bin
         # bins = self.bin_offset + (np.floor((data-self.bin_offset)/self.bin_size)) * self.bin_size
         bins =  ((data-self.bin_offset) // self.bin_size).astype(int)
      else:
         # The data is already binned
         bins = data

      # Sort the bins (get the sorted indices)
      sorted_idx = bins.argsort()

      # Group the bins with the same value
      group_bins, group_start_idx, counts = np.unique(bins[sorted_idx], return_index = True, return_counts = True)
      # Sum the frequencies of the grouped bins
      group_freqs = np.add.reduceat(freqs[sorted_idx], group_start_idx)

      # Performance Timer: Stop
      # self.timer.stop()

      return group_bins, group_freqs


   def load_bins(self, redistribute: bool = False, strict = False):
      # Performance Timer: Start
      # self.timer.start()

      # Check if the bin_size is nan
      if self.bin_size != self.bin_size:
         # Performance Timer: Start
         # self.timer.start("load_bins -> bin_size is nan")
         # Compute the bin size
         max_value = np.nanmax(self._buffer[0: self.n])
         min_value = np.nanmin(self._buffer[0: self.n])
         self.bin_offset = min_value
         self.bin_size = (max_value - min_value)/self.n_bins
         # Make sure the bin_size is not zero
         if self.bin_size < 1e-10:
            # Reset the bin_size
            self.bin_size = np.nan
            self.bin_offset = np.nan
            # All values are duplicates. Keep track of the number of duplicates
            if self.n_dups == 0:
               self.n_dups = self.n
            else:
               self.n_dups += self.n - 1
            # Reset the buffer, keep only the first item (if any)
            x = self._buffer[0]
            self._buffer = np.empty((self.buffer_size), dtype = np.double)
            if self.n:
               self._buffer[0] = x
               self.n = 1

            # Performance Timer: Stop
            # self.timer.stop("load_bins -> bin_size is nan")   
            # Exit
            return
         # Performance Timer: Stop
         # self.timer.stop("load_bins -> bin_size is nan")   

      if redistribute:
         # self.timer.start("load_bins: Dict --> Numpy Arrays")
         data = np.fromiter(self.bins.keys(), dtype = int, count = len(self.bins))
         freqs = np.fromiter(self.bins.values(), dtype = np.uint64, count = len(self.bins))

         # Compute the new bin size
         # max_value = np.nanmax(data)
         # min_value = np.nanmin(data)
         # self.bin_offset = min_value
         # bin_size = (max_value - min_value)/self.n_bins
         # self.bin_size = math.ceil(bin_size/self.bin_size) * self.bin_size
         
         # Compute the new bin size
         max_value = self.bin_offset + np.nanmax(data) * self.bin_size
         min_value = self.bin_offset + np.nanmin(data) * self.bin_size
         # self.bin_offset = min_value
         bin_size = (max_value - min_value)/self.n_bins
         # Compute the growth factor for the new bin size
         if strict:
            growth_factor = math.ceil(bin_size/self.bin_size)
         else:
            growth_factor = min(2, math.ceil(bin_size/self.bin_size))
         # Set the new bin size
         self.bin_size = growth_factor * self.bin_size
         # Remap the data keys to account for the new bin size
         data = (data // growth_factor).astype(int)
         compute_bins = False

         # self.timer.stop("load_bins: Dict --> Numpy Arrays")
      else:
         # Distribute the buffer
         data = self._buffer[0: self.n]
         freqs = np.ones((self.n), dtype = np.uint64)
         compute_bins = True
         freqs[0] = max(1, self.n_dups)
         self.n_dups = 0

      # Group the bins
      group_bins, group_freqs = self.compute_hist(data, freqs, compute_bins = compute_bins)
      # Deallocate the buffer
      self._buffer = None

      # Load the bins and frequencies inside a dictionary
      # self.timer.start("load_bins: Numpy Arrays --> Dict")
      self.bins = dict(zip(group_bins, group_freqs))
      # self.timer.stop("load_bins: Numpy Arrays --> Dict")

      # Performance Timer: Stop
      # self.timer.stop()
      

   def add(self, x: np.double):
      # Performance Timer: Start
      # self.timer.start()

      # In case of NaNs (x != x is equivalent to np.isnan(x), but faster!), update the bin and exit
      if x != x:
         self.bins[np.nan] = self.bins.get(np.nan, 0) + 1
         # Performance Timer: Start
         # self.timer.stop()
         return
      
      # Check if we are past the initialization stage
      if self.n >= self.buffer_size:

         # Compute the center of the bin
         # key = (self.bin_offset + np.floor(x/self.bin_size)) * self.bin_size
         # self.timer.start("add -> compute key")
         # key = self.bin_offset + (math.floor((x-self.bin_offset)/self.bin_size)) * self.bin_size
         key = int((x-self.bin_offset)//self.bin_size)
         # self.timer.stop("add -> compute key")

         # Add the value to the bin
         # self.timer.start("add -> update hash")
         # Get the current count value from the dictionary (if present)
         current_value = self.bins.get(key, 0)
         self.bins[key] = current_value + 1
         # self.timer.stop("add -> update hash")
         self.n += 1

         if current_value == 0 and len(self.bins) > 2*self.n_bins:
            # Reallocate the bins
            self.load_bins(redistribute = True)

      # Check if we are in the initialization stage
      else:
         # Add the value to the buffer
         # self.timer.start("add -> update buffer")
         self._buffer[self.n] = x
         # self.timer.stop("add -> update buffer")
         self.n += 1
         # Check if we have filled the buffer
         if self.n == self.buffer_size:
            # Load the self.bins dictionary
            self.load_bins()

      # Performance Timer: Stop
      # self.timer.stop()


   def load_array(self, x):
      # self.timer.start()
      n_items = len(x)
      freqs = np.ones((n_items), dtype = np.uint64)
      # Group the bins
      group_bins, group_freqs = self.compute_hist(x, freqs)
      # Load the bins and frequencies inside a dictionary
      # self.timer.start("load_array: update hash")
      for key, val in zip(group_bins, group_freqs):
         self.bins[key] = self.bins.get(key, 0) + val
      # self.timer.stop("load_array: update hash")
      # self.timer.stop()


   def add_many(self, x: np.ndarray):
      # self.timer.start()
      if self.n >= self.buffer_size:
         self.load_array(x)
      else:
         n_items = len(x)
         # Determine where to insert the data array inside the buffer
         cutoff_idx = min(self.n + n_items, self.buffer_size)
         n_insert = cutoff_idx - self.n
         self._buffer[self.n:cutoff_idx] = x[0:n_insert]
         self.n += n_insert

         # Check if we have filled the buffer
         if self.n == self.buffer_size:
            # Load the self.bins dictionary
            self.load_bins()

         # Check if there are additional items to process
         if n_insert < n_items:
            self.load_array(x[n_insert:])
            self.n += n_items - n_insert
      # self.timer.stop()

   def histogram(self, n_bins = None):
      # Performance Timer: Start
      # self.timer.start()

      if n_bins:
         self.n_bins = n_bins
         
      self.load_bins(redistribute = self.bins, strict = True)

      if self.n <= 1:
         # Special (degenerate) case, match whatever is produced by np.histogram
         hist, bins = np.histogram(self._buffer[0:self.n])
         hist *= max(1, self.n_dups)
      else:
         # bins = np.fromiter(self.bins.keys(), dtype = np.double, count = len(self.bins))
         hist = np.fromiter(self.bins.values(), dtype = np.uint64, count = len(self.bins))
         bins = self.bin_offset + np.fromiter(self.bins.keys(), dtype = int, count = len(self.bins)) * self.bin_size

         # Sort the entries
         sorted_idx = bins.argsort()
         bins = bins[sorted_idx]
         hist = hist[sorted_idx]

      # Performance Timer: Stop
      # self.timer.stop()

      return hist, bins

