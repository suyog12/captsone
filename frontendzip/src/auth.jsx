import { createContext, useContext, useEffect, useState, useCallback } from 'react';
import * as apiClient from './api.js';

// Auth context

const AuthContext = createContext(null);

export function AuthProvider({ children }) {
  const [user, setUser] = useState(null);
  const [loading, setLoading] = useState(1);

  // Hydrate from existing token on mount
  useEffect(() => {
    const token = apiClient.getToken();
    if (!token) {
      setLoading(0);
      return;
    }
    apiClient
      .getMe()
      .then((u) => setUser(u))
      .catch(() => {
        apiClient.setToken(null);
        setUser(null);
      })
      .finally(() => setLoading(0));
  }, []);

  const login = useCallback(async (username, password) => {
    const resp = await apiClient.login(username, password);
    apiClient.setToken(resp.access_token);
    setUser(resp.user);
    return resp.user;
  }, []);

  const logout = useCallback(() => {
    apiClient.setToken(null);
    setUser(null);
  }, []);

  const value = {
    user,
    loading,
    login,
    logout,
    isAdmin: user && user.role === 'admin',
    isSeller: user && user.role === 'seller',
    isCustomer: user && user.role === 'customer'
  };

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) {
    throw new Error('useAuth must be used inside AuthProvider');
  }
  return ctx;
}
