import AppShell from '../../components/shell/AppShell.jsx';
import CatalogBrowse from '../../components/catalog/CatalogBrowse.jsx';
import { useAuth } from '../../auth.jsx';

// Customer catalog

export default function CustomerCatalog() {
  const { user } = useAuth();
  const custId = user && user.cust_id;

  return (
    <AppShell title="Browse products" subtitle="Explore the full catalog and add items to your cart">
      <CatalogBrowse
        custId={custId}
        allowAddToCart={1}
        ctaSource="manual"
      />
    </AppShell>
  );
}
