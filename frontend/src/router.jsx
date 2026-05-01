import { Routes, Route, Navigate, useLocation } from 'react-router-dom';
import { useAuth } from './auth.jsx';
import LoginPage from './pages/LoginPage.jsx';

// Admin
import AdminOverview from './pages/admin/AdminOverview.jsx';
import AdminRecommendations from './pages/admin/AdminRecommendations.jsx';
import AdminSellers from './pages/admin/AdminSellers.jsx';
import AdminSellerDetail from './pages/admin/AdminSellerDetail.jsx';
import AdminCustomers from './pages/admin/AdminCustomers.jsx';
import AdminCustomerDetail from './pages/admin/AdminCustomerDetail.jsx';
import AdminProducts from './pages/admin/AdminProducts.jsx';
import AdminUserManagement from './pages/admin/AdminUserManagement.jsx';
import AdminCatalog from './pages/admin/AdminCatalog.jsx';

// Seller
import SellerCustomerList from './pages/seller/SellerCustomerList.jsx';
import SellerCustomerProfile from './pages/seller/SellerCustomerProfile.jsx';
import SellerPerformance from './pages/seller/SellerPerformance.jsx';
import SellerCatalog from './pages/seller/SellerCatalog.jsx';

// Customer
import CustomerOverview from './pages/customer/CustomerOverview.jsx';
import CustomerRecommendations from './pages/customer/CustomerRecommendations.jsx';
import CustomerCart from './pages/customer/CustomerCart.jsx';
import CustomerOrders from './pages/customer/CustomerOrders.jsx';
import CustomerCatalog from './pages/customer/CustomerCatalog.jsx';

// Shared (any logged-in user)
import ChangePasswordPage from './pages/ChangePasswordPage.jsx';

// Router

function FullPageLoader() {
  return (
    <div className="flex items-center justify-center min-h-screen bg-mck-navy">
      <div className="text-white text-sm tracking-wide">Loading...</div>
    </div>
  );
}

function RequireAuth({ children, role }) {
  const { user, loading } = useAuth();
  const location = useLocation();
  if (loading) return <FullPageLoader />;
  if (!user) return <Navigate to="/login" replace state={{ from: location }} />;
  if (role && user.role !== role) {
    return <Navigate to={defaultPathForRole(user.role)} replace />;
  }
  return children;
}

function defaultPathForRole(role) {
  if (role === 'admin') return '/admin';
  if (role === 'seller') return '/seller';
  if (role === 'customer') return '/customer';
  return '/login';
}

function HomeRedirect() {
  const { user, loading } = useAuth();
  if (loading) return <FullPageLoader />;
  if (!user) return <Navigate to="/login" replace />;
  return <Navigate to={defaultPathForRole(user.role)} replace />;
}

export default function AppRouter() {
  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />

      {/* Admin */}
      <Route path="/admin" element={<RequireAuth role="admin"><AdminOverview /></RequireAuth>} />
      <Route path="/admin/recommendations" element={<RequireAuth role="admin"><AdminRecommendations /></RequireAuth>} />
      <Route path="/admin/sellers" element={<RequireAuth role="admin"><AdminSellers /></RequireAuth>} />
      <Route path="/admin/sellers/:userId" element={<RequireAuth role="admin"><AdminSellerDetail /></RequireAuth>} />
      <Route path="/admin/customers" element={<RequireAuth role="admin"><AdminCustomers /></RequireAuth>} />
      <Route path="/admin/customers/:custId" element={<RequireAuth role="admin"><AdminCustomerDetail /></RequireAuth>} />
      <Route path="/admin/products" element={<RequireAuth role="admin"><AdminProducts /></RequireAuth>} />
      <Route path="/admin/users" element={<RequireAuth role="admin"><AdminUserManagement /></RequireAuth>} />
      <Route path="/admin/catalog" element={<RequireAuth role="admin"><AdminCatalog /></RequireAuth>} />

      {/* Seller */}
      <Route path="/seller" element={<RequireAuth role="seller"><SellerCustomerList /></RequireAuth>} />
      <Route path="/seller/customers/:custId" element={<RequireAuth role="seller"><SellerCustomerProfile /></RequireAuth>} />
      <Route path="/seller/performance" element={<RequireAuth role="seller"><SellerPerformance /></RequireAuth>} />
      <Route path="/seller/customers/:custId/catalog" element={<RequireAuth role="seller"><SellerCatalog /></RequireAuth>} />

      {/* Customer */}
      <Route path="/customer" element={<RequireAuth role="customer"><CustomerOverview /></RequireAuth>} />
      <Route path="/customer/recommendations" element={<RequireAuth role="customer"><CustomerRecommendations /></RequireAuth>} />
      <Route path="/customer/cart" element={<RequireAuth role="customer"><CustomerCart /></RequireAuth>} />
      <Route path="/customer/orders" element={<RequireAuth role="customer"><CustomerOrders /></RequireAuth>} />
      <Route path="/customer/catalog" element={<RequireAuth role="customer"><CustomerCatalog /></RequireAuth>} />

      {/* Shared - any logged-in user */}
      <Route path="/account/password" element={<RequireAuth><ChangePasswordPage /></RequireAuth>} />

      <Route path="/" element={<HomeRedirect />} />
      <Route path="*" element={<HomeRedirect />} />
    </Routes>
  );
}
