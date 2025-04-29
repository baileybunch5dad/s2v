#include <iostream>

#include <unordered_map>

#include <vector>

#include <cmath>

#include <limits>

#include <algorithm>

#include <numeric>

#include <memory>

#include <cstring>

 

class DynamicDist {

public:

    DynamicDist(int n_bins = 1000, int buffer_size = 100000, double bin_size = std::numeric_limits<double>::quiet_NaN(),

                double bin_offset = std::numeric_limits<double>::quiet_NaN(), int rebalance_factor = 3)

                : buffer_size(buffer_size), n_bins(n_bins), n(0), n_dups(0),

                  bin_size(bin_size), bin_offset(bin_offset), rebalance_factor(rebalance_factor) {

 

        _buffer.resize(buffer_size);

        _inv_bin_size = 1.0 / bin_size;

    }

 

    void compute_hist(const std::vector<double>& data, const std::vector<uint64_t>* freqs, bool compute_bins, std::vector<int>& group_bins, std::vector<int>& group_freqs) {

        std::vector<int> bins(data.size());

        if (compute_bins) {

            for (size_t i = 0; i < data.size(); ++i) {

                bins[i] = static_cast<int>((data[i] - bin_offset) * _inv_bin_size);

            }

        } else {

            std::copy(data.begin(), data.end(), bins.begin());

        }

 

        std::unordered_map<int, uint64_t> counts;

        for (size_t i = 0; i < bins.size(); ++i) {

            counts[bins[i]] += (freqs ? (*freqs)[i] : 1);

        }

 

        group_bins.clear();

        group_freqs.clear();

        for (const auto& pair : counts) {

            group_bins.push_back(pair.first);

            group_freqs.push_back(pair.second);

        }

    }

 

    void load_bins(bool redistribute = false, bool strict = false) {

        if (n == 0) return;

 

        if (std::isnan(bin_size)) {

            double max_value = *std::max_element(_buffer.begin(), _buffer.begin() + n);

            double min_value = *std::min_element(_buffer.begin(), _buffer.begin() + n);

            bin_offset = min_value;

            bin_size = (max_value - min_value) / n_bins;

 

            if (std::isnan(bin_size) || bin_size < 1e-15) {

                bin_size = std::numeric_limits<double>::quiet_NaN();

                bin_offset = std::numeric_limits<double>::quiet_NaN();

                n_dups += (n_dups == 0) ? n : n - 1;

                _buffer.resize(1);

                _buffer[0] = _buffer[0];

                n = 1;

                return;

            } else {

                _inv_bin_size = 1.0 / bin_size;

            }

        }

 

        uint64_t nan_freq = 0;

        if (redistribute) {

            auto it = bins.find(std::numeric_limits<double>::quiet_NaN());

            if (it != bins.end()) {

                nan_freq = it->second;

                bins.erase(it);

            }

 

            std::vector<int> data;

            std::vector<uint64_t> freqs;

            for (const auto& pair : bins) {

                data.push_back(pair.first);

                freqs.push_back(pair.second);

            }

 

            double max_value = bin_offset + *std::max_element(data.begin(), data.end()) * bin_size;

            double min_value = bin_offset + *std::min_element(data.begin(), data.end()) * bin_size;

            double new_bin_size = (max_value - min_value) / n_bins;

 

            int growth_factor = strict ? (int)std::ceil(new_bin_size / bin_size) : (int)std::round(new_bin_size / bin_size);

            bin_size *= growth_factor;

            _inv_bin_size = 1.0 / bin_size;

 

            for (auto& d : data) {

                d /= growth_factor;

            }

            compute_bins = false;

        } else {

            std::vector<int> data(_buffer.begin(), _buffer.begin() + n);

            std::vector<uint64_t> freqs;

            compute_bins = true;

            if (n_dups > 0) {

                freqs.resize(n, 1);

                freqs[0] = n_dups;

                n_dups = 0;

            }

            _buffer.clear();

        }

 

        std::vector<int> group_bins, group_freqs;

        compute_hist(data, freqs.empty() ? nullptr : &freqs, compute_bins, group_bins, group_freqs);

       

        bins.clear();

        for (size_t i = 0; i < group_bins.size(); ++i) {

            bins[group_bins[i]] += group_freqs[i];

        }

 

        if (nan_freq) {

            bins[std::numeric_limits<double>::quiet_NaN()] += nan_freq;

        }

    }

 

    void add(double x) {

        if (!std::isnan(bin_size)) {

            int key = static_cast<int>((x - bin_offset) * _inv_bin_size);

            bins[key]++;

            n++;

 

            if (bins[key] == 1 && bins.size() > rebalance_factor * n_bins) {

                load_bins(true);

            }

        } else {

            _buffer[n++] = x;

            if (n == buffer_size) {

                load_bins();

            }

        }

    }

 

    std::tuple<std::vector<double>, std::vector<uint64_t>, uint64_t> get_merge_data(bool centered = true) {

        uint64_t nan_freq = 0;

        auto it = bins.find(std::numeric_limits<double>::quiet_NaN());

        if (it != bins.end()) {

            nan_freq = it->second;

            bins.erase(it);

        }

 

        std::vector<double> hist, bins_vec;

        for (const auto& pair : bins) {

            hist.push_back(pair.second);

            bins_vec.push_back(bin_offset + pair.first * bin_size + 0.5 * bin_size * centered);

        }

 

        return {bins_vec, hist, nan_freq};

    }

 

    void merge(const std::vector<double>& data, const std::vector<uint64_t>* freqs = nullptr, uint64_t nan_freq = 0) {

        std::vector<int> group_bins, group_freqs;

        compute_hist(data, freqs, true, group_bins, group_freqs);

       

        for (size_t i = 0; i < group_bins.size(); ++i) {

            bins[group_bins[i]] += group_freqs[i];

        }

 

        if (nan_freq) {

            bins[std::numeric_limits<double>::quiet_NaN()] += nan_freq;

        }

    }

 

    void add_many(const std::vector<double>& data) {

        size_t n_items = data.size();

        if (n >= buffer_size) {

            merge(data);

            n += n_items;

        } else {

            size_t cutoff_idx = std::min(n + n_items, buffer_size);

            size_t n_insert = cutoff_idx - n;

            std::copy(data.begin(), data.begin() + n_insert, _buffer.begin() + n);

            n += n_insert;

 

            if (n == buffer_size) {

                load_bins();

            }

           

            if (n_insert < n_items) {

                merge(std::vector<double>(data.begin() + n_insert, data.end()));

                n += n_items - n_insert;

            }

        }

 

        if (bins.size() > rebalance_factor * n_bins) {

            load_bins(true);

        }

    }

 

    std::tuple<std::vector<double>, std::vector<uint64_t>> histogram(int n_bins = 0, bool strict = true, bool include_nan = false, bool centered = true) {

        if (n_bins) {

            this->n_bins = n_bins;

        }

 

        load_bins(!bins.empty(), strict);

 

        std::vector<uint64_t> hist;

        std::vector<double> bins_vec;

 

        if (n <= 1) {

            hist.resize(1, (n_dups > 0) ? n_dups : 1);

            bins_vec = {0.0}; // Placeholder

        } else {

            uint64_t nan_freq = 0;

            auto it = bins.find(std::numeric_limits<double>::quiet_NaN());

            if (it != bins.end()) {

                nan_freq = it->second;

                bins.erase(it);

            }

 

            for (const auto& pair : bins) {

                hist.push_back(pair.second);

                bins_vec.push_back(bin_offset + pair.first * bin_size + 0.5 * bin_size * centered);

            }

 

            // Sort

            std::vector<size_t> indices(hist.size());

            std::iota(indices.begin(), indices.end(), 0);

            std::sort(indices.begin(), indices.end(), [&](size_t a, size_t b) { return bins_vec[a] < bins_vec[b]; });

 

            std::vector<double> sorted_bins(bins_vec.size());

            std::vector<uint64_t> sorted_hist(hist.size());

 

            for (size_t i = 0; i < indices.size(); ++i) {

                sorted_bins[i] = bins_vec[indices[i]];

                sorted_hist[i] = hist[indices[i]];

            }

 

            bins_vec = sorted_bins;

            hist = sorted_hist;

 

            if (include_nan) {

                bins_vec.push_back(std::numeric_limits<double>::quiet_NaN());

                hist.push_back(nan_freq);

            }

        }

        return {bins_vec, hist};

    }

 

private:

    int buffer_size;

    int n_bins;

    int n;

    int n_dups;

    double bin_size;

    double bin_offset;

    int rebalance_factor;

    double _inv_bin_size;

    std::vector<double> _buffer;

    std::unordered_map<double, uint64_t> bins; // Map to hold the bins

};