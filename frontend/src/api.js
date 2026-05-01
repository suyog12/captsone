import axios from 'axios';

// Axios client

const BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000';

export const api = axios.create({
  baseURL: BASE_URL,
  timeout: 20000
});

// Token plumbing

let _token = sessionStorage.getItem('mck_token') || null;

export function setToken(token) {
  _token = token;
  if (token) {
    sessionStorage.setItem('mck_token', token);
  } else {
    sessionStorage.removeItem('mck_token');
  }
}

export function getToken() {
  return _token;
}

api.interceptors.request.use((config) => {
  if (_token) {
    config.headers.Authorization = `Bearer ${_token}`;
  }
  return config;
});

api.interceptors.response.use(
  (resp) => resp,
  (err) => {
    if (err.response && err.response.status === 401) {
      setToken(null);
      if (window.location.pathname !== '/login') {
        window.location.href = '/login';
      }
    }
    return Promise.reject(err);
  }
);

// Auth

export async function login(username, password) {
  const form = new URLSearchParams();
  form.append('username', username);
  form.append('password', password);
  const { data } = await api.post('/auth/login', form, {
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' }
  });
  return data;
}

export async function getMe() {
  const { data } = await api.get('/auth/me');
  return data;
}

export async function changePassword(current_password, new_password) {
  const { data } = await api.patch('/users/me/password', { current_password, new_password });
  return data;
}

// Users (admin)

export async function listUsers(params = {}) {
  const { data } = await api.get('/users', { params });
  return data;
}

export async function getUser(userId) {
  const { data } = await api.get(`/users/${userId}`);
  return data;
}

export async function createAdmin(payload) {
  const { data } = await api.post('/users/admins', payload);
  return data;
}

export async function createSeller(payload) {
  const { data } = await api.post('/users/sellers', payload);
  return data;
}

export async function createCustomer(payload) {
  const { data } = await api.post('/users/customers', payload);
  return data;
}

export async function deactivateUser(userId) {
  const { data } = await api.delete(`/users/${userId}`);
  return data;
}

export async function reactivateUser(userId) {
  const { data } = await api.post(`/users/${userId}/reactivate`);
  return data;
}

// Customers
//
// searchCustomers accepts an optional `scope` parameter:
//   undefined or 'mine' (default) - sellers see only their assigned customers
//   'all'                          - sellers can search the whole base (used by
//                                    the seller's "All customers" tab)
// Admins ignore scope; they always search the full base.
export async function searchCustomers(q, limit = 25, scope = undefined) {
  const params = { q, limit };
  if (scope) params.scope = scope;
  const { data } = await api.get('/customers/search', { params });
  return data;
}

export async function filterCustomers(params = {}) {
  const { data } = await api.get('/customers/filter', { params });
  return data;
}

export async function getMyCustomerRecord() {
  const { data } = await api.get('/customers/me');
  return data;
}

export async function getCustomer(custId) {
  const { data } = await api.get(`/customers/${custId}`);
  return data;
}

export async function getCustomerHistory(custId, params = {}) {
  const { data } = await api.get(`/customers/${custId}/history`, { params });
  return data;
}

export async function getCustomerStats(custId, params = {}) {
  const { data } = await api.get(`/customers/${custId}/stats`, { params });
  return data;
}

// Recommendations

export async function getMyRecommendations(n = 10) {
  const { data } = await api.get('/recommendations/me', { params: { n } });
  return data;
}

export async function getCustomerRecommendations(custId, n = 20) {
  const { data } = await api.get(`/recommendations/customer/${custId}`, { params: { n } });
  return data;
}

export async function getCartHelper(custId, cartItems) {
  const { data } = await api.post('/recommendations/cart-helper', {
    cust_id: custId,
    cart_items: cartItems
  });
  return data;
}

// Cart

export async function addToCart(custId, item_id, quantity = 1, source = 'manual') {
  const { data } = await api.post(`/customers/${custId}/cart`, { item_id, quantity, source });
  return data;
}

export async function getCart(custId) {
  const { data } = await api.get(`/customers/${custId}/cart`);
  return data;
}

export async function getCartHistory(custId, params = {}) {
  const { data } = await api.get(`/customers/${custId}/cart/history`, { params });
  return data;
}

export async function getMyCart() {
  const { data } = await api.get('/cart/me');
  return data;
}

export async function updateCartQuantity(cartItemId, quantity) {
  const { data } = await api.patch(`/cart/${cartItemId}`, { quantity });
  return data;
}

export async function deleteCartItem(cartItemId) {
  const { data } = await api.delete(`/cart/${cartItemId}`);
  return data;
}

export async function updateCartStatus(cartItemId, status) {
  const { data } = await api.patch(`/cart/${cartItemId}/status`, { status });
  return data;
}

export async function checkoutCartItem(cartItemId) {
  const { data } = await api.post(`/cart/${cartItemId}/checkout`);
  return data;
}

// Assignments

export async function changeAssignment(custId, seller_id, notes) {
  const { data } = await api.patch(`/customers/${custId}/assignment`, { seller_id, notes });
  return data;
}

export async function claimCustomer(custId) {
  const { data } = await api.post(`/customers/${custId}/claim`, {});
  return data;
}

export async function bulkAssign(seller_id, cust_ids, notes) {
  const { data } = await api.post('/customers/assignments/bulk', { seller_id, cust_ids, notes });
  return data;
}

export async function getAssignmentHistory(custId, limit = 100) {
  const { data } = await api.get(`/customers/${custId}/assignment-history`, { params: { limit } });
  return data;
}

// Sellers

export async function getMyCustomers(limit = 100, offset = 0) {
  const { data } = await api.get('/sellers/me/customers', { params: { limit, offset } });
  return data;
}

export async function getSellerCustomers(userId, limit = 100, offset = 0) {
  const { data } = await api.get(`/sellers/${userId}/customers`, { params: { limit, offset } });
  return data;
}

export async function getMySellerStats(params = {}) {
  const { data } = await api.get('/sellers/me/stats', { params });
  return data;
}

export async function getMyConversionBySignal() {
  const { data } = await api.get('/sellers/me/conversion-by-signal');
  return data;
}

// Admin stats

export async function getAdminOverview() {
  const { data } = await api.get('/admin/stats/overview');
  return data;
}

export async function getSalesTrend(params = {}) {
  const { data } = await api.get('/admin/stats/sales-trend', { params });
  return data;
}

export async function getConversionBySignal() {
  const { data } = await api.get('/admin/stats/conversion-by-signal');
  return data;
}

export async function getSegmentDistribution() {
  const { data } = await api.get('/admin/stats/segment-distribution');
  return data;
}

export async function getTopSellers(params = {}) {
  const { data } = await api.get('/admin/stats/top-sellers', { params });
  return data;
}

export async function getRecentSales(params = {}) {
  const { data } = await api.get('/admin/stats/recent-sales', { params });
  return data;
}

// Products - catalog browse
export async function browseProducts(params = {}) {
  const { data } = await api.get('/products', { params });
  return data;
}

export async function getProductFilters() {
  const { data } = await api.get('/products/filters');
  return data;
}

// Admin: top customers by revenue
export async function getTopCustomers(params = {}) {
  // params: limit, range
  const { data } = await api.get('/admin/stats/top-customers', { params });
  return data;
}

// Recommendations - reject (seller-only)
export async function rejectRecommendation(payload) {
  // payload: { cust_id, item_id, primary_signal, rec_purpose, reason_code, reason_note }
  const { data } = await api.post('/recommendations/reject', payload);
  return data;
}

// Admin: engine effectiveness funnel
export async function getEngineEffectiveness() {
  const { data } = await api.get('/admin/stats/engine-effectiveness');
  return data;
}

// Customers: create a customer record without a login (seller auto-assigns
// to themselves; admins can pass assigned_seller_id explicitly)
export async function createCustomerRecord(payload) {
  // payload: {
  //   customer_business_name, market_code, size_tier, specialty_code,
  //   assigned_seller_id (admin only, null for self-assigned seller flow)
  // }
  const { data } = await api.post('/customers/record', payload);
  return data;
}

// Users: change my own password
export async function changeMyPassword(currentPassword, newPassword) {
  const { data } = await api.patch('/users/me/password', {
    current_password: currentPassword,
    new_password: newPassword
  });
  return data;
}

// Admin: customer lifecycle distribution (churn funnel)
export async function getChurnFunnel() {
  const { data } = await api.get('/admin/stats/churn-funnel');
  return data;
}