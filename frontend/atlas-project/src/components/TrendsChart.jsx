import React, { useState, useEffect } from 'react';
import apiService from '../services/apiServices';

const TrendsChart = () => {
  const [trendsData, setTrendsData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [selectedMetric, setSelectedMetric] = useState('total_cases');

  useEffect(() => {
    fetchYearlyTrends();
  }, []);

  const fetchYearlyTrends = async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await apiService.getYearlyTrends();
      setTrendsData(data);
    } catch (err) {
      setError(`Failed to load trends: ${err.message}`);
      console.error('Yearly trends fetch error:', err);
    } finally {
      setLoading(false);
    }
  };

  const metrics = [
    { key: 'total_cases', label: 'Total Cases', color: 'blue' },
    { key: 'new_cases', label: 'New Cases', color: 'green' },
    { key: 'deaths', label: 'Deaths', color: 'red' },
    { key: 'avg_cases_per_100k', label: 'Avg Cases per 100k', color: 'purple' }
  ];

  const getMetricColor = (metric) => {
    const colors = {
      blue: '#3b82f6',
      green: '#10b981',
      red: '#ef4444',
      purple: '#8b5cf6'
    };
    return colors[metrics.find(m => m.key === metric)?.color] || '#6b7280';
  };

  const formatValue = (value, metric) => {
    if (!value && value !== 0) return 'N/A';
    
    if (metric === 'avg_cases_per_100k' || metric === 'avg_case_fatality_rate') {
      return value.toFixed(1);
    }
    
    return value.toLocaleString();
  };

  if (loading) {
    return (
      <div className="bg-white rounded-2xl shadow-lg border border-gray-200 p-6">
        <div className="flex items-center justify-center h-64">
          <div className="text-center">
            <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto mb-4"></div>
            <p className="text-gray-600">Loading trends data...</p>
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
            <p className="font-semibold">Error loading trends data</p>
            <p className="text-sm mt-2">{error}</p>
            <button
              onClick={fetchYearlyTrends}
              className="mt-4 px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors"
            >
              Retry
            </button>
          </div>
        </div>
      </div>
    );
  }

  if (!trendsData || !trendsData.trends || trendsData.trends.length === 0) {
    return (
      <div className="bg-white rounded-2xl shadow-lg border border-gray-200 p-6">
        <div className="flex items-center justify-center h-64">
          <div className="text-center text-gray-500">
            <div className="text-4xl mb-4">📊</div>
            <p className="font-semibold">No trends data available</p>
          </div>
        </div>
      </div>
    );
  }

  const trends = trendsData.trends;
  const maxValue = Math.max(...trends.map(t => t[selectedMetric] || 0));

  return (
    <div className="bg-white rounded-2xl shadow-lg border border-gray-200 p-6">
      {/* Header */}
      <div className="mb-6">
        <h2 className="text-2xl font-semibold text-gray-800 mb-2">Regional TB Trends</h2>
        <p className="text-gray-600">Southeast Asia yearly aggregated data</p>
      </div>

      {/* Metric Selector */}
      <div className="mb-6">
        <div className="flex flex-wrap gap-2">
          {metrics.map(metric => (
            <button
              key={metric.key}
              onClick={() => setSelectedMetric(metric.key)}
              className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                selectedMetric === metric.key
                  ? `bg-${metric.color}-100 text-${metric.color}-700 border border-${metric.color}-200`
                  : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
              }`}
            >
              {metric.label}
            </button>
          ))}
        </div>
      </div>

      {/* Chart */}
      <div className="mb-6">
        <div className="bg-gray-50 rounded-lg p-4">
          <div className="flex items-end justify-between h-48 space-x-2">
            {trends.map((trend, index) => {
              const value = trend[selectedMetric] || 0;
              const height = maxValue > 0 ? (value / maxValue) * 100 : 0;
              
              return (
                <div key={trend.year} className="flex-1 flex flex-col items-center">
                  <div className="w-full flex flex-col items-center">
                    {/* Bar */}
                    <div
                      className="w-full rounded-t transition-all duration-300 hover:opacity-80"
                      style={{
                        height: `${Math.max(2, height)}%`,
                        backgroundColor: getMetricColor(selectedMetric),
                        minHeight: '4px'
                      }}
                      title={`${trend.year}: ${formatValue(value, selectedMetric)}`}
                    ></div>
                    
                    {/* Value label */}
                    <div className="text-xs text-gray-600 mt-1 text-center">
                      {formatValue(value, selectedMetric)}
                    </div>
                    
                    {/* Year label */}
                    <div className="text-xs font-medium text-gray-800 mt-1">
                      {trend.year}
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>

      {/* Summary Statistics */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div className="bg-blue-50 rounded-lg p-4 text-center">
          <div className="text-2xl font-bold text-blue-600">
            {trendsData.summary?.total_years || trends.length}
          </div>
          <div className="text-sm text-gray-600">Years of Data</div>
        </div>
        
        <div className="bg-green-50 rounded-lg p-4 text-center">
          <div className="text-2xl font-bold text-green-600">
            {trends.length > 0 ? formatValue(trends[trends.length - 1][selectedMetric], selectedMetric) : 'N/A'}
          </div>
          <div className="text-sm text-gray-600">Latest Value</div>
        </div>
        
        <div className="bg-purple-50 rounded-lg p-4 text-center">
          <div className="text-2xl font-bold text-purple-600">
            {formatValue(maxValue, selectedMetric)}
          </div>
          <div className="text-sm text-gray-600">Peak Value</div>
        </div>
        
        <div className="bg-orange-50 rounded-lg p-4 text-center">
          <div className="text-2xl font-bold text-orange-600">
            {trends.length > 1 ? (
              trends[trends.length - 1][selectedMetric] > trends[trends.length - 2][selectedMetric] ? '↗️' : '↘️'
            ) : '➖'}
          </div>
          <div className="text-sm text-gray-600">Trend</div>
        </div>
      </div>

      {/* Data Table */}
      <div className="mt-6">
        <h3 className="text-lg font-semibold text-gray-800 mb-3">Data Table</h3>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-50">
                <th className="text-left p-2 font-medium text-gray-700">Year</th>
                <th className="text-right p-2 font-medium text-gray-700">Total Cases</th>
                <th className="text-right p-2 font-medium text-gray-700">New Cases</th>
                <th className="text-right p-2 font-medium text-gray-700">Deaths</th>
                <th className="text-right p-2 font-medium text-gray-700">Avg per 100k</th>
              </tr>
            </thead>
            <tbody>
              {trends.map((trend, index) => (
                <tr key={trend.year} className={index % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                  <td className="p-2 font-medium">{trend.year}</td>
                  <td className="p-2 text-right">{formatValue(trend.total_cases, 'total_cases')}</td>
                  <td className="p-2 text-right">{formatValue(trend.new_cases, 'new_cases')}</td>
                  <td className="p-2 text-right">{formatValue(trend.deaths, 'deaths')}</td>
                  <td className="p-2 text-right">{formatValue(trend.avg_cases_per_100k, 'avg_cases_per_100k')}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};

export default TrendsChart;

