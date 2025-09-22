import React, { useState, useEffect } from 'react';
import apiService from '../services/apiServices';

const ComparisonTable = ({ selectedYear = '2023' }) => {
  const [comparisonData, setComparisonData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [sortConfig, setSortConfig] = useState({ key: 'total_cases', direction: 'desc' });

  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      setError(null);
      try {
        const data = await apiService.getRegionalComparison(parseInt(selectedYear));
        setComparisonData(data);
      } catch (err) {
        setError(`Failed to load comparison data: ${err.message}`);
        console.error('Comparison data fetch error:', err);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [selectedYear]);

  const fetchComparisonData = async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await apiService.getRegionalComparison(parseInt(selectedYear));
      setComparisonData(data);
    } catch (err) {
      setError(`Failed to load comparison data: ${err.message}`);
      console.error('Comparison data fetch error:', err);
    } finally {
      setLoading(false);
    }
  };

  const handleSort = (key) => {
    let direction = 'desc';
    if (sortConfig.key === key && sortConfig.direction === 'desc') {
      direction = 'asc';
    }
    setSortConfig({ key, direction });
  };

  const getSortedData = () => {
    if (!comparisonData || !comparisonData.countries) return [];
    
    const sortedCountries = [...comparisonData.countries].sort((a, b) => {
      const aValue = a[sortConfig.key] || 0;
      const bValue = b[sortConfig.key] || 0;
      
      if (sortConfig.direction === 'asc') {
        return aValue - bValue;
      }
      return bValue - aValue;
    });
    
    return sortedCountries;
  };

  const formatValue = (value, type = 'number') => {
    if (!value && value !== 0) return 'N/A';
    
    if (type === 'percentage') {
      return `${value.toFixed(2)}%`;
    }
    
    if (type === 'decimal') {
      return value.toFixed(1);
    }
    
    return value.toLocaleString();
  };

  const getSortIcon = (columnKey) => {
    if (sortConfig.key !== columnKey) {
      return <span className="text-gray-400">↕️</span>;
    }
    return sortConfig.direction === 'asc' ? <span className="text-blue-600">↑</span> : <span className="text-blue-600">↓</span>;
  };

  const getRankBadge = (rank) => {
    const colors = {
      1: 'bg-yellow-100 text-yellow-800 border-yellow-200',
      2: 'bg-gray-100 text-gray-800 border-gray-200',
      3: 'bg-orange-100 text-orange-800 border-orange-200'
    };
    
    return colors[rank] || 'bg-blue-100 text-blue-800 border-blue-200';
  };

  if (loading) {
    return (
      <div className="bg-white rounded-2xl shadow-lg border border-gray-200 p-6">
        <div className="flex items-center justify-center h-64">
          <div className="text-center">
            <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto mb-4"></div>
            <p className="text-gray-600">Loading comparison data...</p>
          </div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-white rounded-2xl shadow-lg border border-gray-200 p-6">
        <div className="flex items-center justify-center h-64">
          <div className="text-center text-red-600">
            <div className="text-4xl mb-4">⚠️</div>
            <p className="font-semibold">Error loading comparison data</p>
            <p className="text-sm mt-2">{error}</p>
            <button
              onClick={fetchComparisonData}
              className="mt-4 px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors"
            >
              Retry
            </button>
          </div>
        </div>
      </div>
    );
  }

  if (!comparisonData || !comparisonData.countries || comparisonData.countries.length === 0) {
    return (
      <div className="bg-white rounded-2xl shadow-lg border border-gray-200 p-6">
        <div className="flex items-center justify-center h-64">
          <div className="text-center text-gray-500">
            <div className="text-4xl mb-4">📊</div>
            <p className="font-semibold">No comparison data available</p>
          </div>
        </div>
      </div>
    );
  }

  const sortedCountries = getSortedData();

  return (
    <div className="bg-white rounded-2xl shadow-lg border border-gray-200 p-6">
      {/* Header */}
      <div className="mb-6">
        <h2 className="text-2xl font-semibold text-gray-800 mb-2">Regional Comparison</h2>
        <p className="text-gray-600">TB statistics comparison for {comparisonData.year}</p>
      </div>

      {/* Rankings Summary */}
      {comparisonData.rankings && (
        <div className="mb-6 grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="bg-yellow-50 rounded-lg p-4">
            <h3 className="font-semibold text-yellow-800 mb-2">Highest Total Cases</h3>
            <div className="space-y-1">
              {comparisonData.rankings.highest_cases?.slice(0, 3).map((country, index) => (
                <div key={country.iso3} className="flex justify-between text-sm">
                  <span className="text-yellow-700">{index + 1}. {country.country}</span>
                  <span className="font-medium">{formatValue(country.total_cases)}</span>
                </div>
              ))}
            </div>
          </div>

          <div className="bg-red-50 rounded-lg p-4">
            <h3 className="font-semibold text-red-800 mb-2">Highest Deaths</h3>
            <div className="space-y-1">
              {comparisonData.rankings.highest_deaths?.slice(0, 3).map((country, index) => (
                <div key={country.iso3} className="flex justify-between text-sm">
                  <span className="text-red-700">{index + 1}. {country.country}</span>
                  <span className="font-medium">{formatValue(country.deaths)}</span>
                </div>
              ))}
            </div>
          </div>

          <div className="bg-purple-50 rounded-lg p-4">
            <h3 className="font-semibold text-purple-800 mb-2">Highest Rate per 100k</h3>
            <div className="space-y-1">
              {comparisonData.rankings.highest_rate?.slice(0, 3).map((country, index) => (
                <div key={country.iso3} className="flex justify-between text-sm">
                  <span className="text-purple-700">{index + 1}. {country.country}</span>
                  <span className="font-medium">{formatValue(country.total_cases_per_100k, 'decimal')}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Data Table */}
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-gray-50">
              <th className="text-left p-3 font-medium text-gray-700">
                <button
                  onClick={() => handleSort('country')}
                  className="flex items-center space-x-1 hover:text-blue-600"
                >
                  <span>Country</span>
                  {getSortIcon('country')}
                </button>
              </th>
              <th className="text-right p-3 font-medium text-gray-700">
                <button
                  onClick={() => handleSort('total_cases')}
                  className="flex items-center space-x-1 hover:text-blue-600 ml-auto"
                >
                  <span>Total Cases</span>
                  {getSortIcon('total_cases')}
                </button>
              </th>
              <th className="text-right p-3 font-medium text-gray-700">
                <button
                  onClick={() => handleSort('new_cases')}
                  className="flex items-center space-x-1 hover:text-blue-600 ml-auto"
                >
                  <span>New Cases</span>
                  {getSortIcon('new_cases')}
                </button>
              </th>
              <th className="text-right p-3 font-medium text-gray-700">
                <button
                  onClick={() => handleSort('deaths')}
                  className="flex items-center space-x-1 hover:text-blue-600 ml-auto"
                >
                  <span>Deaths</span>
                  {getSortIcon('deaths')}
                </button>
              </th>
              <th className="text-right p-3 font-medium text-gray-700">
                <button
                  onClick={() => handleSort('total_cases_per_100k')}
                  className="flex items-center space-x-1 hover:text-blue-600 ml-auto"
                >
                  <span>Cases per 100k</span>
                  {getSortIcon('total_cases_per_100k')}
                </button>
              </th>
              <th className="text-right p-3 font-medium text-gray-700">
                <button
                  onClick={() => handleSort('case_fatality_rate')}
                  className="flex items-center space-x-1 hover:text-blue-600 ml-auto"
                >
                  <span>Fatality Rate</span>
                  {getSortIcon('case_fatality_rate')}
                </button>
              </th>
            </tr>
          </thead>
          <tbody>
            {sortedCountries.map((country, index) => (
              <tr 
                key={country.iso3} 
                className={`${index % 2 === 0 ? 'bg-white' : 'bg-gray-50'} hover:bg-blue-50 transition-colors`}
              >
                <td className="p-3">
                  <div className="flex items-center space-x-2">
                    <span 
                      className={`inline-flex items-center px-2 py-1 rounded-full text-xs font-medium border ${getRankBadge(index + 1)}`}
                    >
                      {index + 1}
                    </span>
                    <div>
                      <div className="font-medium text-gray-900">{country.country}</div>
                      <div className="text-gray-500 text-xs">{country.iso3}</div>
                    </div>
                  </div>
                </td>
                <td className="p-3 text-right font-medium">{formatValue(country.total_cases)}</td>
                <td className="p-3 text-right">{formatValue(country.new_cases)}</td>
                <td className="p-3 text-right text-red-600">{formatValue(country.deaths)}</td>
                <td className="p-3 text-right">{formatValue(country.total_cases_per_100k, 'decimal')}</td>
                <td className="p-3 text-right">{formatValue(country.case_fatality_rate, 'percentage')}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Summary */}
      <div className="mt-6 pt-4 border-t border-gray-200">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-center">
          <div className="bg-blue-50 rounded-lg p-3">
            <div className="text-lg font-bold text-blue-600">
              {comparisonData.total_countries || sortedCountries.length}
            </div>
            <div className="text-xs text-gray-600">Countries</div>
          </div>
          <div className="bg-green-50 rounded-lg p-3">
            <div className="text-lg font-bold text-green-600">
              {formatValue(sortedCountries.reduce((sum, c) => sum + (c.total_cases || 0), 0))}
            </div>
            <div className="text-xs text-gray-600">Total Cases</div>
          </div>
          <div className="bg-red-50 rounded-lg p-3">
            <div className="text-lg font-bold text-red-600">
              {formatValue(sortedCountries.reduce((sum, c) => sum + (c.deaths || 0), 0))}
            </div>
            <div className="text-xs text-gray-600">Total Deaths</div>
          </div>
          <div className="bg-purple-50 rounded-lg p-3">
            <div className="text-lg font-bold text-purple-600">
              {formatValue(
                sortedCountries.reduce((sum, c) => sum + (c.total_cases_per_100k || 0), 0) / sortedCountries.length,
                'decimal'
              )}
            </div>
            <div className="text-xs text-gray-600">Avg per 100k</div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default ComparisonTable;

