import React, { useState, useEffect } from 'react';
import apiService from '../services/apiServices';

const CountryInfoBox = ({ selectedCountry, onClose, onYearChange }) => {
  const [trends, setTrends] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (selectedCountry && selectedCountry.iso3) {
      fetchCountryTrends(selectedCountry.iso3);
    }
  }, [selectedCountry]);

  const fetchCountryTrends = async (iso3) => {
    setLoading(true);
    setError(null);
    try {
      const data = await apiService.getCountryTrends(iso3);
      setTrends(data);
    } catch (err) {
      setError(`Failed to load trends: ${err.message}`);
      console.error('Country trends fetch error:', err);
    } finally {
      setLoading(false);
    }
  };

  if (!selectedCountry) return null;

  const currentData = selectedCountry.data || {};

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
      <div className="bg-white rounded-2xl shadow-2xl max-w-2xl w-full max-h-[90vh] overflow-y-auto">
        {/* Header */}
        <div className="bg-gradient-to-r from-blue-600 to-purple-600 text-white p-6 rounded-t-2xl">
          <div className="flex justify-between items-start">
            <div>
              <h2 className="text-2xl font-bold">{selectedCountry.country}</h2>
              <p className="text-blue-100 mt-1">TB Statistics for {currentData.year || 'N/A'}</p>
            </div>
            <button
              onClick={onClose}
              className="text-white hover:text-gray-200 transition-colors p-2 hover:bg-white/20 rounded-lg"
            >
              <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>
        </div>

        {/* Content */}
        <div className="p-6">
          {/* Current Year Statistics */}
          <div className="mb-6">
            <h3 className="text-lg font-semibold text-gray-800 mb-4">Current Statistics</h3>
            <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
              <div className="bg-blue-50 rounded-lg p-4">
                <div className="text-2xl font-bold text-blue-600">
                  {currentData.total_cases?.toLocaleString() || 'N/A'}
                </div>
                <div className="text-sm text-gray-600">Total Cases</div>
              </div>
              <div className="bg-green-50 rounded-lg p-4">
                <div className="text-2xl font-bold text-green-600">
                  {currentData.new_cases?.toLocaleString() || 'N/A'}
                </div>
                <div className="text-sm text-gray-600">New Cases</div>
              </div>
              <div className="bg-red-50 rounded-lg p-4">
                <div className="text-2xl font-bold text-red-600">
                  {currentData.deaths?.toLocaleString() || 'N/A'}
                </div>
                <div className="text-sm text-gray-600">Deaths</div>
              </div>
              <div className="bg-purple-50 rounded-lg p-4">
                <div className="text-2xl font-bold text-purple-600">
                  {currentData.total_cases_per_100k?.toFixed(1) || 'N/A'}
                </div>
                <div className="text-sm text-gray-600">Cases per 100k</div>
              </div>
              <div className="bg-orange-50 rounded-lg p-4">
                <div className="text-2xl font-bold text-orange-600">
                  {currentData.new_cases_per_100k?.toFixed(1) || 'N/A'}
                </div>
                <div className="text-sm text-gray-600">New per 100k</div>
              </div>
              <div className="bg-gray-50 rounded-lg p-4">
                <div className="text-2xl font-bold text-gray-600">
                  {currentData.case_fatality_rate?.toFixed(2) || 'N/A'}%
                </div>
                <div className="text-sm text-gray-600">Fatality Rate</div>
              </div>
            </div>
          </div>

          {/* Historical Trends */}
          <div className="mb-6">
            <h3 className="text-lg font-semibold text-gray-800 mb-4">Historical Trends</h3>
            
            {loading && (
              <div className="flex items-center justify-center py-8">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
                <span className="ml-2 text-gray-600">Loading trends...</span>
              </div>
            )}

            {error && (
              <div className="bg-red-50 border border-red-200 rounded-lg p-4 text-red-600">
                {error}
              </div>
            )}

            {trends && trends.trends && (
              <div className="space-y-4">
                {/* Simple trend visualization */}
                <div className="bg-gray-50 rounded-lg p-4">
                  <h4 className="font-medium text-gray-700 mb-3">Total Cases Over Time</h4>
                  <div className="space-y-2">
                    {trends.trends.slice(-5).map((trend, index) => (
                      <div key={trend.year} className="flex items-center justify-between">
                        <span className="text-sm text-gray-600">{trend.year}</span>
                        <div className="flex items-center">
                          <div 
                            className="bg-blue-500 h-2 rounded mr-2"
                            style={{ 
                              width: `${Math.max(10, (trend.total_cases / Math.max(...trends.trends.map(t => t.total_cases))) * 100)}px` 
                            }}
                          ></div>
                          <span className="text-sm font-medium text-gray-800">
                            {trend.total_cases?.toLocaleString() || 'N/A'}
                          </span>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>

                {/* Summary */}
                <div className="bg-blue-50 rounded-lg p-4">
                  <h4 className="font-medium text-blue-800 mb-2">Trend Summary</h4>
                  <div className="text-sm text-blue-700 space-y-1">
                    <p>Data available for {trends.summary?.years_available || 0} years</p>
                    <p>Latest year: {trends.summary?.latest_year || 'N/A'}</p>
                    <p>Latest cases: {trends.summary?.latest_cases?.toLocaleString() || 'N/A'}</p>
                  </div>
                </div>
              </div>
            )}
          </div>

          {/* Year Selector */}
          <div className="border-t pt-4">
            <h3 className="text-lg font-semibold text-gray-800 mb-4">View Different Year</h3>
            <div className="flex flex-wrap gap-2">
              {[2018, 2019, 2020, 2021, 2022, 2023].map(year => (
                <button
                  key={year}
                  onClick={() => {
                    onYearChange(year.toString());
                    onClose();
                  }}
                  className="px-4 py-2 bg-gray-100 hover:bg-blue-100 text-gray-700 hover:text-blue-700 rounded-lg transition-colors text-sm font-medium"
                >
                  {year}
                </button>
              ))}
            </div>
          </div>
        </div>

        {/* Footer */}
        <div className="bg-gray-50 px-6 py-4 rounded-b-2xl">
          <div className="flex justify-between items-center">
            <div className="text-sm text-gray-500">
              ISO3: {selectedCountry.iso3}
            </div>
            <button
              onClick={onClose}
              className="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg transition-colors text-sm font-medium"
            >
              Close
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default CountryInfoBox;

