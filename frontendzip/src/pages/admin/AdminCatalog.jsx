import AppShell from '../../components/shell/AppShell.jsx';
import CatalogBrowse from '../../components/catalog/CatalogBrowse.jsx';

// Admin catalog

export default function AdminCatalog() {
  return (
    <AppShell
      title="Product catalog"
      subtitle="Browse the full inventory. Stock levels are live."
    >
      <CatalogBrowse
        custId={null}
        allowAddToCart={0}
      />
    </AppShell>
  );
}
