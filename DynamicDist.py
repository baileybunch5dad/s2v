from typing import Dict
import numpy as np
import math


class DynamicDist:

   def __init__(self
                , n_bins: int = 1000
                , buffer_size: int = 100000
                , bin_size: np.double = np.nan
                , bin_offset: np.double = np.nan
                , rebalance_factor: int = 3
                , data: np.ndarray[np.double] = None
                , freqs:np.ndarray[np.double] = None
                , nan_freq: int = None
                , serialized = False
                ):

      # Size of the buffer used to hold the initial set of samples
      self.buffer_size: int = buffer_size
      # Number of bins
      self.n_bins: int = n_bins
      # Number of samples processed
      self.n: int = 0
      # Number of duplicate values
      self.n_dups: int = 0
      # Bin Size
      self.bin_size: np.double = bin_size
      # Dictionary to hold all the bins
      self.bins: Dict[np.double, int] = dict()
      # Bin offset
      self.bin_offset: np.double = bin_offset
      # Rebalance factor
      self.rebalance_factor = rebalance_factor

      # Initialize the buffer
      self._buffer: list = [None] * self.buffer_size

      # Keep track of the inverse bin size (for performance reasons)
      self._inv_bin_size: np.double = 1.0/self.bin_size

      # Load the initial data
      if data is not None:
         # Check if the bin size has been set
         if np.isnan(self.bin_size):
            # Data is not binned. Just add it to the buffer
            self.add_many(data)
         else:
            # Check if the bin_offset is nan
            if np.isnan(self.bin_offset):
               # Set the bin offset
               self.bin_offset = np.nanmin(data)
            # Load the binned data into the self.bins dictionary
            self.merge(data = data, freqs = freqs, nan_freq = nan_freq, compute_bins = not serialized, group_by = not serialized)


   # Serialization method
   def __reduce__(self):

      # Get internal data and frequencies for serialization
      data, freqs, nan_freq = self.get_merge_data(centered = False)

      # Return the class and the arguments to reconstruct the object
      return (self.__class__
               , (self.n_bins
                  , self.buffer_size
                  , self.bin_size
                  , self.bin_offset
                  , self.rebalance_factor
                  , data
                  , freqs
                  , nan_freq
                  , True # serialized
                  )
               , None
            )


   def compute_hist(self, data, freqs = None, compute_bins = True, group_by = True):

      if compute_bins:
         # Map each data point to a bin
         bins = ((data-self.bin_offset) * self._inv_bin_size).astype(int)
      else:
         # The data is already binned
         bins = data

      if compute_bins or group_by:
         # Aggregate the bins with the same value
         group_bins, inverse = np.unique(bins, return_inverse = True)
         group_freqs = np.bincount(inverse, weights = freqs).astype(int)
      else:
         return bins, freqs

      return group_bins, group_freqs


   def load_bins(self, redistribute: bool = False, strict = False):
      # Exit if there is nothing to distribute
      if self.n == 0:
         return
      
      # Check if the bin_size is nan
      if self.bin_size != self.bin_size:
         # Compute the bin size
         max_value = np.nanmax(self._buffer[0: self.n])
         min_value = np.nanmin(self._buffer[0: self.n])
         self.bin_offset = min_value
         self.bin_size = (max_value - min_value)/self.n_bins
         # Special processing if bin_size is NaN or zero
         if self.bin_size != self.bin_size or self.bin_size < 1e-15:
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
            self._buffer = [None] * self.buffer_size
            self._buffer[0] = x
            self.n = 1

            # Exit
            return
         else:
            self._inv_bin_size = 1.0 / self.bin_size

      nan_freq = None
      if redistribute:
         # Extract the frequency of NaN entries (if any)
         nan_freq = self.bins.pop(np.nan, None)
         # Load the bin keys and values into numpy arrays
         data = np.fromiter(self.bins.keys(), dtype = int, count = len(self.bins))
         freqs = np.fromiter(self.bins.values(), dtype = np.uint64, count = len(self.bins))
         
         # Compute the new bin size
         max_value = self.bin_offset + np.nanmax(data) * self.bin_size
         min_value = self.bin_offset + np.nanmin(data) * self.bin_size
         bin_size = (max_value - min_value)/self.n_bins
         # Compute the growth factor for the new bin size
         if strict:
            growth_factor = math.ceil(bin_size/self.bin_size)
         else:
            # growth_factor = 2**math.floor(math.log2(bin_size/self.bin_size))
            growth_factor = round(bin_size/self.bin_size)
         # Set the new bin size
         self.bin_size = growth_factor * self.bin_size
         self._inv_bin_size = 1.0 / self.bin_size
         # Remap the data keys to account for the new bin size
         data = (data // growth_factor).astype(int)
         compute_bins = False
      else:
         # Distribute the buffer
         data = np.array(self._buffer[0: self.n])
         freqs = None
         compute_bins = True
         if self.n_dups > 0:
            freqs = np.ones((self.n), dtype = np.uint64)
            freqs[0] = self.n_dups
            self.n_dups = 0
         # Deallocate the buffer
         self._buffer = None

      # Group the bins
      group_bins, group_freqs = self.compute_hist(data, freqs, compute_bins = compute_bins)

      # Load the bins and frequencies inside a dictionary
      self.bins = dict(zip(group_bins, group_freqs))

      # Restore the NaN key
      if nan_freq:
         self.bins[np.nan] = nan_freq


   def add(self, x: np.double):      
      # Check if we are past the initialization stage 
      #   self.bin_size == self.bin_size is equivalent to ! np.isnan(self.bin_size), but faster
      if self.bin_size == self.bin_size:
         # Compute the index of the bin where to insert the data
         key = int((x-self.bin_offset) * self._inv_bin_size)
         # Get the current count value from the dictionary (if present)
         current_value = self.bins.get(key, 0)
         # Add the value to the bin
         self.bins[key] = current_value + 1
         self.n += 1

         if current_value == 0 and len(self.bins) > self.rebalance_factor*self.n_bins:
            # Reallocate the bins
            self.load_bins(redistribute = True)

      # Check if we are in the initialization stage
      else:
         # Add the value to the buffer
         self._buffer[self.n] = x
         self.n += 1
         # Check if we have filled the buffer
         if self.n == self.buffer_size:
            # Load the self.bins dictionary
            self.load_bins()


   def get_merge_data(self, centered = True):
      # Check if we are past the initialization stage 
      #   self.bin_size == self.bin_size is equivalent to ! np.isnan(self.bin_size), but faster
      if self.bin_size == self.bin_size:
         # Extract the frequency of NaN entries (if any)
         nan_freq = self.bins.pop(np.nan, None)
         # Create Numpy arrays from the keys and values of the dictionary
         hist = np.fromiter(self.bins.values(), dtype = np.uint64, count = len(self.bins))
         bins = self.bin_offset + np.fromiter(self.bins.keys(), dtype = int, count = len(self.bins)) * self.bin_size + 0.5*self.bin_size*centered
      else:
         # Get the raw data from the buffer and return it as a numpy array
         bins = np.array(self._buffer[0:self.n])
         hist = None
         nan_freq = None

      # Return the result
      return bins, hist, nan_freq

   def merge(self, data, freqs = None, nan_freq = None, compute_bins = True, group_by = True):
      # Check if we are past the initialization stage
      #   self.bin_size == self.bin_size is equivalent to ! np.isnan(self.bin_size), but faster
      if self.bin_size == self.bin_size:
         # Group the bins
         group_bins, group_freqs = self.compute_hist(data = data, freqs = freqs, compute_bins = compute_bins, group_by = group_by)

         # Merge the grouped bins and frequencies inside the bins dictionary
         for key, val in zip(group_bins, group_freqs):
            self.bins[key] = self.bins.get(key, 0) + val

         # Update the number of samples processed
         self.n += np.sum(group_freqs)

         # Keep track of NaN values (if any)
         if nan_freq:
            self.bins[np.nan] = self.bins.get(np.nan, 0) + nan_freq
            self.n += nan_freq
      else:
         # This is an edge case. You should not try to merge binned data if we are in the initialization stage
         if freqs is not None and np.max(freqs) > 1:
            # Print a warning: Merging binned data while in the initialization stage
            Warning("Merging binned data while in the initialization stage. This might affect accuracy!")
            # Create the bins from the buffer
            self.load_bins()
            # Now merge the data
            self.merge(data = data, freqs = freqs, nan_freq = nan_freq)
         else:
            self.add_many(data)

   def add_many(self, data: np.ndarray):
      n_items = len(data)
      if self.n >= self.buffer_size:
         self.merge(data)
      else:
         # Determine where to insert the data array inside the buffer
         cutoff_idx = min(self.n + n_items, self.buffer_size)
         n_insert = cutoff_idx - self.n
         self._buffer[self.n:cutoff_idx] = data[0:n_insert]
         self.n += n_insert

         # Check if we have filled the buffer
         if self.n == self.buffer_size:
            # Load the self.bins dictionary
            self.load_bins()

         # Check if there are additional items to process
         if n_insert < n_items:
            self.merge(data[n_insert:])

      if len(self.bins) > self.rebalance_factor*self.n_bins:
         # Reallocate the bins
         self.load_bins(redistribute = True)
      
   def histogram(self, n_bins = None, strict = True, include_nan = False, centered = True):
      if n_bins:
         self.n_bins = n_bins
         
      self.load_bins(redistribute = self.bins, strict = strict)

      if self.n <= 1:
         # Special (degenerate) case, match whatever is produced by np.histogram
         hist, bins = np.histogram(np.nan_to_num(self._buffer[0:self.n], nan = 0))
         hist *= max(1, self.n_dups)
         bins = bins[:-1]
      else:
         # Extract the frequency of NaN entries (if any)
         nan_freq = self.bins.pop(np.nan, None)
         hist = np.fromiter(self.bins.values(), dtype = np.uint64, count = len(self.bins))
         bins = self.bin_offset + np.fromiter(self.bins.keys(), dtype = int, count = len(self.bins)) * self.bin_size + 0.5*self.bin_size*centered

         # Sort the entries
         sorted_idx = bins.argsort()
         bins = bins[sorted_idx]
         hist = hist[sorted_idx]

         if include_nan:
            bins = np.append(bins, np.nan)
            hist = np.append(hist, nan_freq)

      return bins, hist

   @classmethod
   def merge_many(cls, objects: list['DynamicDist'] = None):

      # Create a new instance of the class
      result = cls()

      # Make sure we have a list of objects
      if isinstance(objects, cls):
         objects = [objects]

      if objects:
         if len(objects) == 1:
            # Just return the first object
            return objects[0]
         else:
            # Get a list of all the bin_sizes
            bin_sizes = [obj.bin_size for obj in objects]
            # Get the sorted indices of bin_sizes
            sorted_idx = np.argsort(bin_sizes)
            # Get the first object
            result = objects[sorted_idx[0]]
            for idx in sorted_idx[1:]:
               # Get the current object
               obj = objects[idx]
               if np.isnan(result.bin_size) and np.isnan(obj.bin_size):
                  # Both objects are not binned. Get the data from the first object
                  data, freqs, nan_freq = obj.get_merge_data()
                  # Add the data to the result object
                  result.add_many(data)
               elif np.isnan(obj.bin_size) or result.bin_size >= obj.bin_size:
                  # Merge the current object into the result object
                  result.merge(*obj.get_merge_data())
               else:
                  # Merge the result into the current object
                  obj.merge(*result.get_merge_data())
                  # Set the result to the current object
                  result = obj

      # If still in sample space prepare bins for tail_risk
      if np.isnan(result.bin_size):
         # Compute the bin size
         result.load_bins()

      # Return the merged object
      return result