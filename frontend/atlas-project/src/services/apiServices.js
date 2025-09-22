import axios from 'axios';

// Base URL for the Flask API
const BASE_URL = 'http://localhost:5000/api';

// Create axios instance with default configuration
const apiClient = axios.create({
  baseURL: BASE_URL,
  timeout: 10000,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Request interceptor for logging
apiClient.interceptors.request.use(
  (config) => {
    console.log(`Making ${config.method?.toUpperCase()} request to ${config.url}`);
    return config;
  },
  (error) => {
    console.error('Request error:', error);
    return Promise.reject(error);
  }
);

// Response interceptor for error handling
apiClient.interceptors.response.use(
  (response) => {
    console.log(`Response received from ${response.config.url}:`, response.status);
    return response;
  },
  (error) => {
    console.error('Response error:', error);
    
    // Handle different error scenarios
    if (error.response) {
      // Server responded with error status
      const { status, data } = error.response;
      console.error(`API Error ${status}:`, data);
      throw new Error(data.message || `Server error: ${status}`);
    } else if (error.request) {
      // Request made but no response received
      console.error('Network error - no response received');
      throw new Error('Network error: Unable to connect to server');
    } else {
      // Something else happened
      console.error('Request setup error:', error.message);
      throw new Error(`Request error: ${error.message}`);
    }
  }
);

const apiService = {
  /**
   * Get map data for visualization
   * @param {number} year - Year to fetch data for (optional)
   * @returns {Promise} Map data with features and regional stats
   */
  async getMapData(year = null) {
    try {
      const params = year ? { year } : {};
      const response = await apiClient.get('/map-data', { params });
      return response.data;
    } catch (error) {
      console.error('Error fetching map data:', error);
      throw error;
    }
  },

  /**
   * Get country trends data
   * @param {string} iso3 - Country ISO3 code
   * @param {number} startYear - Start year (optional)
   * @param {number} endYear - End year (optional)
   * @returns {Promise} Country trends data
   */
  async getCountryTrends(iso3, startYear = null, endYear = null) {
    try {
      const params = {};
      if (startYear) params.start_year = startYear;
      if (endYear) params.end_year = endYear;
      
      const response = await apiClient.get(`/trends/${iso3}`, { params });
      return response.data;
    } catch (error) {
      console.error(`Error fetching trends for ${iso3}:`, error);
      throw error;
    }
  },

  /**
   * Get regional comparison data
   * @param {number} year - Year to fetch data for (optional)
   * @returns {Promise} Regional comparison data
   */
  async getRegionalComparison(year = null) {
    try {
      const params = year ? { year } : {};
      const response = await apiClient.get('/comparison', { params });
      return response.data;
    } catch (error) {
      console.error('Error fetching regional comparison:', error);
      throw error;
    }
  },

  /**
   * Get yearly trends data
   * @returns {Promise} Yearly aggregated trends
   */
  async getYearlyTrends() {
    try {
      const response = await apiClient.get('/yearly-trends');
      return response.data;
    } catch (error) {
      console.error('Error fetching yearly trends:', error);
      throw error;
    }
  },

  /**
   * Get list of available countries
   * @returns {Promise} List of countries
   */
  async getCountries() {
    try {
      const response = await apiClient.get('/countries');
      return response.data;
    } catch (error) {
      console.error('Error fetching countries:', error);
      throw error;
    }
  },

  /**
   * Health check endpoint
   * @returns {Promise} Health status
   */
  async healthCheck() {
    try {
      const response = await apiClient.get('/health');
      return response.data;
    } catch (error) {
      console.error('Error checking API health:', error);
      throw error;
    }
  }
};

export default apiService;

