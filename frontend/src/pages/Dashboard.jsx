import { useState, useEffect, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { motion, AnimatePresence } from "framer-motion";
import {
  LayoutGrid,
  Activity,
  Key,
  Download,
  LogOut,
  Sun,
  Moon,
  RefreshCw,
  Plus,
  X,
  Terminal,
  CheckCircle2,
  XCircle,
  Clock,
  Loader2,
  AlertTriangle,
  Pause,
  Filter,
  ChevronLeft,
  ChevronRight,
  ExternalLink,
  Copy,
  Trash2,
  Users,
  GitCompare,
} from "lucide-react";
import { useAuth } from "../context/AuthContext";
import { useTheme } from "../context/ThemeContext";
import { taskAPI, monitoringAPI, exportAPI, authAPI } from "../utils/api";
import { formatDistanceToNow, format } from "date-fns";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
} from "recharts";

// Status badge component
const StatusBadge = ({ status }) => {
  const config = {
    pending: {
      bg: "bg-zinc-100 dark:bg-zinc-800",
      text: "text-zinc-600 dark:text-zinc-400",
      border: "border-zinc-200 dark:border-zinc-700",
    },
    queued: {
      bg: "bg-blue-100 dark:bg-blue-500/10",
      text: "text-blue-800 dark:text-blue-400",
      border: "border-blue-200 dark:border-blue-500/20",
    },
    started: {
      bg: "bg-blue-100 dark:bg-blue-500/10",
      text: "text-blue-800 dark:text-blue-400",
      border: "border-blue-200 dark:border-blue-500/20",
    },
    running: {
      bg: "bg-blue-100 dark:bg-blue-500/10",
      text: "text-blue-800 dark:text-blue-400",
      border: "border-blue-200 dark:border-blue-500/20",
    },
    completed: {
      bg: "bg-green-100 dark:bg-green-500/10",
      text: "text-green-800 dark:text-green-400",
      border: "border-green-200 dark:border-green-500/20",
    },
    failed: {
      bg: "bg-red-100 dark:bg-red-500/10",
      text: "text-red-800 dark:text-red-400",
      border: "border-red-200 dark:border-red-500/20",
    },
    retrying: {
      bg: "bg-yellow-100 dark:bg-yellow-500/10",
      text: "text-yellow-800 dark:text-yellow-400",
      border: "border-yellow-200 dark:border-yellow-500/20",
    },
    cancelled: {
      bg: "bg-zinc-100 dark:bg-zinc-800",
      text: "text-zinc-600 dark:text-zinc-400",
      border: "border-zinc-200 dark:border-zinc-700",
    },
  };

  const style = config[status] || config.pending;

  return (
    <span
      className={`font-mono text-xs px-2 py-0.5 border ${style.bg} ${style.text} ${style.border} uppercase`}
    >
      {status}
    </span>
  );
};

// Metric Card component
const MetricCard = ({ label, value, icon: Icon, color = "default" }) => {
  const colorClasses = {
    default: "text-zinc-950 dark:text-white",
    success: "text-green-600 dark:text-green-400",
    danger: "text-red-600 dark:text-red-400",
    warning: "text-yellow-600 dark:text-yellow-400",
    info: "text-blue-600 dark:text-blue-400",
  };

  return (
    <div className="p-4 md:p-6 border border-zinc-200 dark:border-zinc-800 bg-zinc-50 dark:bg-zinc-900">
      <div className="flex items-start justify-between">
        <div>
          <p className="font-mono text-xs uppercase tracking-[0.2em] text-zinc-500 dark:text-zinc-400 mb-2">
            {label}
          </p>
          <p
            className={`font-mono text-3xl md:text-4xl font-bold tracking-tighter ${colorClasses[color]}`}
          >
            {value}
          </p>
        </div>
        {Icon && (
          <Icon
            className="text-zinc-400 dark:text-zinc-600"
            size={24}
            strokeWidth={1.5}
          />
        )}
      </div>
    </div>
  );
};

// Terminal Log component
const TerminalLog = ({ logs }) => {
  return (
    <div className="bg-zinc-950 p-4 font-mono text-sm overflow-auto max-h-64 border border-zinc-800">
      {logs.length === 0 ? (
        <div className="text-zinc-500">
          <span className="text-green-400">$</span> Waiting for task activity...
          <span className="animate-blink">█</span>
        </div>
      ) : (
        <AnimatePresence>
          {logs.map((log, idx) => (
            <motion.div
              key={log.id || idx}
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              className={`mb-1 ${
                log.type === "success"
                  ? "text-green-400"
                  : log.type === "error"
                    ? "text-red-400"
                    : log.type === "warning"
                      ? "text-yellow-400"
                      : "text-zinc-400"
              }`}
            >
              <span className="text-zinc-600">[{log.time}]</span> {log.message}
            </motion.div>
          ))}
        </AnimatePresence>
      )}
    </div>
  );
};

// Create Task Modal
const CreateTaskModal = ({
  isOpen,
  onClose,
  onSubmit,
  activeSiteTasks = [],
}) => {
  const [formData, setFormData] = useState({
    site: "",
    max_retries: 3,
    tags: "",
    payload: "{}",
  });
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [sites, setSites] = useState([]);
  const [loadingSites, setLoadingSites] = useState(false);
  const [error, setError] = useState(null);

  // Check if selected site has an active task
  const selectedSiteHasActiveTask =
    formData.site && activeSiteTasks.includes(formData.site);

  // Fetch scrapers when modal opens
  useEffect(() => {
    if (isOpen) {
      setError(null); // Reset error when modal opens
      if (sites.length === 0) {
        const fetchScrapers = async () => {
          setLoadingSites(true);
          try {
            const response = await monitoringAPI.scrapers();
            setSites(
              response.data.map((s) => ({
                site_id: s.site_id,
                name: s.name,
              })),
            );
          } catch (err) {
            console.error("Failed to fetch scrapers:", err);
            // Fallback to hardcoded list if API fails
            setSites([
              { site_id: "hackernews", name: "Hacker News" },
              { site_id: "printify", name: "Printify" },
            ]);
          } finally {
            setLoadingSites(false);
          }
        };
        fetchScrapers();
      }
    }
  }, [isOpen, sites.length]);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError(null);
    setIsSubmitting(true);
    try {
      let payload = {};
      try {
        payload = JSON.parse(formData.payload);
      } catch {
        payload = {};
      }

      await onSubmit({
        site: formData.site,
        max_retries: parseInt(formData.max_retries),
        tags: formData.tags
          ? formData.tags.split(",").map((t) => t.trim())
          : [],
        payload,
      });
      onClose();
      setFormData({ site: "", max_retries: 3, tags: "", payload: "{}" });
    } catch (err) {
      console.error("Failed to create task:", err);
      // Handle 409 Conflict for site concurrency
      if (err.response?.status === 409) {
        setError(
          err.response?.data?.detail ||
            "A task for this site is already in progress. Please wait for it to complete or cancel it.",
        );
      } else {
        setError(
          err.response?.data?.detail ||
            "Failed to create task. Please try again.",
        );
      }
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleClose = () => {
    setError(null);
    setFormData({ site: "", max_retries: 3, tags: "", payload: "{}" });
    onClose();
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        className="w-full max-w-lg bg-white dark:bg-zinc-900 border border-zinc-200 dark:border-zinc-800"
      >
        <div className="flex items-center justify-between p-4 border-b border-zinc-200 dark:border-zinc-800">
          <h3 className="font-heading text-lg font-bold tracking-tight text-zinc-950 dark:text-white">
            Create Scraping Task
          </h3>
          <button
            onClick={handleClose}
            className="p-1 hover:bg-zinc-100 dark:hover:bg-zinc-800 transition-colors"
            data-testid="close-modal-btn"
          >
            <X size={20} strokeWidth={1.5} />
          </button>
        </div>

        <form onSubmit={handleSubmit} className="p-4 space-y-4">
          {/* Error Message */}
          {error && (
            <div
              className="p-3 bg-red-100 dark:bg-red-500/10 border border-red-200 dark:border-red-500/20 flex items-start gap-2"
              data-testid="task-error-message"
            >
              <AlertTriangle
                size={16}
                className="text-red-600 dark:text-red-400 mt-0.5 flex-shrink-0"
              />
              <p className="font-mono text-sm text-red-800 dark:text-red-400">
                {error}
              </p>
            </div>
          )}

          <div>
            <label className="font-mono text-xs uppercase tracking-[0.15em] text-zinc-600 dark:text-zinc-400 mb-2 block">
              Site *
            </label>
            <select
              value={formData.site}
              onChange={(e) => {
                setFormData({ ...formData, site: e.target.value });
                setError(null);
              }}
              required
              disabled={loadingSites}
              className="w-full px-4 py-3 font-mono text-sm bg-white dark:bg-zinc-950 border border-zinc-300 dark:border-zinc-700 rounded-none text-zinc-950 dark:text-white focus:outline-none focus:ring-2 focus:ring-black dark:focus:ring-white disabled:opacity-50"
              data-testid="task-site-select"
            >
              <option value="">
                {loadingSites ? "Loading scrapers..." : "Select a site..."}
              </option>
              {sites.map((s) => (
                <option key={s.site_id} value={s.site_id}>
                  {s.name}{" "}
                  {activeSiteTasks.includes(s.site_id) ? "(Task Running)" : ""}
                </option>
              ))}
            </select>
            {/* Active Task Warning */}
            {selectedSiteHasActiveTask && (
              <div
                className="mt-2 p-2 bg-yellow-100 dark:bg-yellow-500/10 border border-yellow-200 dark:border-yellow-500/20 flex items-center gap-2"
                data-testid="site-active-warning"
              >
                <Loader2
                  size={14}
                  className="text-yellow-600 dark:text-yellow-400 animate-spin"
                />
                <p className="font-mono text-xs text-yellow-800 dark:text-yellow-400">
                  This site already has a task running. You can proceed but it
                  will be blocked until the current task completes.
                </p>
              </div>
            )}
          </div>

          <div className="grid grid-cols-1 gap-4">
            <div>
              <label className="font-mono text-xs uppercase tracking-[0.15em] text-zinc-600 dark:text-zinc-400 mb-2 block">
                Max Retries
              </label>
              <input
                type="number"
                min="0"
                max="10"
                value={formData.max_retries}
                onChange={(e) =>
                  setFormData({ ...formData, max_retries: e.target.value })
                }
                className="w-full px-4 py-3 font-mono text-sm bg-white dark:bg-zinc-950 border border-zinc-300 dark:border-zinc-700 rounded-none text-zinc-950 dark:text-white focus:outline-none focus:ring-2 focus:ring-black dark:focus:ring-white"
                data-testid="task-retries-input"
              />
            </div>
          </div>

          <div>
            <label className="font-mono text-xs uppercase tracking-[0.15em] text-zinc-600 dark:text-zinc-400 mb-2 block">
              Tags (comma-separated)
            </label>
            <input
              type="text"
              value={formData.tags}
              onChange={(e) =>
                setFormData({ ...formData, tags: e.target.value })
              }
              placeholder="product, urgent"
              className="w-full px-4 py-3 font-mono text-sm bg-white dark:bg-zinc-950 border border-zinc-300 dark:border-zinc-700 rounded-none text-zinc-950 dark:text-white placeholder-zinc-400 focus:outline-none focus:ring-2 focus:ring-black dark:focus:ring-white"
              data-testid="task-tags-input"
            />
          </div>

          <div className="flex gap-3 pt-4">
            <button
              type="button"
              onClick={handleClose}
              className="flex-1 py-3 bg-white dark:bg-zinc-950 border border-zinc-200 dark:border-zinc-800 text-zinc-950 dark:text-white font-mono font-medium text-sm hover:bg-zinc-100 dark:hover:bg-zinc-800 transition-colors"
              data-testid="cancel-task-btn"
            >
              CANCEL
            </button>
            <button
              type="submit"
              disabled={isSubmitting}
              className="flex-1 py-3 bg-zinc-950 dark:bg-white text-white dark:text-zinc-950 font-mono font-bold text-sm hover:bg-zinc-800 dark:hover:bg-zinc-200 disabled:opacity-50 transition-colors flex items-center justify-center gap-2"
              data-testid="submit-task-btn"
            >
              {isSubmitting ? (
                <>
                  <Loader2 size={16} className="animate-spin" />
                  CREATING...
                </>
              ) : (
                "CREATE TASK"
              )}
            </button>
          </div>
        </form>
      </motion.div>
    </div>
  );
};

// API Keys Panel
const ApiKeysPanel = ({ isOpen, onClose }) => {
  const [keys, setKeys] = useState([]);
  const [newKeyName, setNewKeyName] = useState("");
  const [createdKey, setCreatedKey] = useState(null);
  const [loading, setLoading] = useState(false);
  const [copiedKeyId, setCopiedKeyId] = useState(null);

  const fetchKeys = useCallback(async () => {
    try {
      const response = await authAPI.listApiKeys();
      setKeys(response.data);
    } catch (err) {
      console.error("Failed to fetch keys:", err);
    }
  }, []);

  useEffect(() => {
    if (isOpen) {
      // Reset state when modal opens
      setCreatedKey(null);
      setNewKeyName("");
      setCopiedKeyId(null);
      fetchKeys();
    }
  }, [isOpen, fetchKeys]);

  const handleCreate = async () => {
    if (!newKeyName.trim()) return;
    setLoading(true);
    try {
      const response = await authAPI.createApiKey(newKeyName);
      setCreatedKey(response.data.api_key);
      setNewKeyName("");
      fetchKeys();
    } catch (err) {
      console.error("Failed to create key:", err);
    } finally {
      setLoading(false);
    }
  };

  const handleDelete = async (id) => {
    try {
      await authAPI.deleteApiKey(id);
      fetchKeys();
    } catch (err) {
      console.error("Failed to delete key:", err);
    }
  };

  const copyToClipboard = (text, keyId = null) => {
    navigator.clipboard.writeText(text);
    if (keyId) {
      setCopiedKeyId(keyId);
      setTimeout(() => setCopiedKeyId(null), 2000);
    }
  };

  // Mask API key for display - show first 8 and last 4 chars
  const maskApiKey = (key) => {
    if (!key || key.length < 16) return "••••••••••••••••";
    return `${key.slice(0, 8)}${"•".repeat(16)}${key.slice(-4)}`;
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        className="w-full max-w-2xl bg-white dark:bg-zinc-900 border border-zinc-200 dark:border-zinc-800 max-h-[80vh] overflow-hidden flex flex-col"
      >
        <div className="flex items-center justify-between p-4 border-b border-zinc-200 dark:border-zinc-800">
          <h3 className="font-heading text-lg font-bold tracking-tight text-zinc-950 dark:text-white">
            API Keys Management
          </h3>
          <button
            onClick={onClose}
            className="p-1 hover:bg-zinc-100 dark:hover:bg-zinc-800 transition-colors"
            data-testid="close-keys-panel-btn"
          >
            <X size={20} strokeWidth={1.5} />
          </button>
        </div>

        <div className="p-4 border-b border-zinc-200 dark:border-zinc-800 space-y-4">
          {createdKey && (
            <div className="p-4 bg-green-100 dark:bg-green-500/10 border border-green-200 dark:border-green-500/20">
              <p className="font-mono text-xs text-green-800 dark:text-green-400 mb-2 flex items-center gap-2">
                <CheckCircle2 size={14} />
                NEW KEY CREATED - COPY AND SAVE IT NOW (shown only once)
              </p>
              <div className="flex items-center gap-2">
                <code className="flex-1 font-mono text-sm bg-white dark:bg-zinc-950 p-2 border break-all text-zinc-950 dark:text-white">
                  {createdKey}
                </code>
                <button
                  onClick={() => copyToClipboard(createdKey)}
                  className="p-2 hover:bg-green-200 dark:hover:bg-green-500/20 transition-colors text-green-700 dark:text-green-400"
                  data-testid="copy-new-key-btn"
                >
                  <Copy size={16} />
                </button>
              </div>
            </div>
          )}

          <div className="flex gap-2">
            <input
              type="text"
              value={newKeyName}
              onChange={(e) => setNewKeyName(e.target.value)}
              placeholder="Key name (e.g., production-scraper)"
              className="flex-1 px-4 py-2 font-mono text-sm bg-white dark:bg-zinc-950 border border-zinc-300 dark:border-zinc-700 rounded-none text-zinc-950 dark:text-white placeholder-zinc-400 focus:outline-none focus:ring-2 focus:ring-black dark:focus:ring-white"
              data-testid="new-key-name-input"
            />
            <button
              onClick={handleCreate}
              disabled={loading || !newKeyName.trim()}
              className="px-4 py-2 bg-zinc-950 dark:bg-white text-white dark:text-zinc-950 font-mono font-bold text-sm hover:bg-zinc-800 dark:hover:bg-zinc-200 disabled:opacity-50 transition-colors flex items-center gap-2"
              data-testid="create-key-btn"
            >
              {loading ? (
                <Loader2 size={14} className="animate-spin" />
              ) : (
                <Plus size={14} />
              )}
              CREATE
            </button>
          </div>
        </div>

        <div className="flex-1 overflow-auto p-4">
          {keys.length === 0 ? (
            <div className="text-center py-12">
              <Key
                size={32}
                className="mx-auto text-zinc-300 dark:text-zinc-700 mb-3"
              />
              <p className="font-mono text-sm text-zinc-500">
                No API keys found
              </p>
              <p className="font-mono text-xs text-zinc-400 mt-1">
                Create your first API key above
              </p>
            </div>
          ) : (
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-zinc-200 dark:border-zinc-800">
                  <th className="text-left font-mono text-xs uppercase tracking-[0.15em] text-zinc-500 py-2">
                    Name
                  </th>
                  <th className="text-left font-mono text-xs uppercase tracking-[0.15em] text-zinc-500 py-2">
                    Key Prefix
                  </th>
                  <th className="text-left font-mono text-xs uppercase tracking-[0.15em] text-zinc-500 py-2">
                    Created
                  </th>
                  <th className="text-left font-mono text-xs uppercase tracking-[0.15em] text-zinc-500 py-2">
                    Status
                  </th>
                  <th className="text-right font-mono text-xs uppercase tracking-[0.15em] text-zinc-500 py-2">
                    Actions
                  </th>
                </tr>
              </thead>
              <tbody>
                {keys.map((key) => (
                  <tr
                    key={key.id}
                    className="border-b border-zinc-100 dark:border-zinc-800/50"
                  >
                    <td className="py-3 font-mono text-zinc-950 dark:text-white font-medium">
                      {key.name}
                    </td>
                    <td className="py-3">
                      <div className="flex items-center gap-2">
                        <code className="font-mono text-xs text-zinc-500 dark:text-zinc-400 bg-zinc-100 dark:bg-zinc-800 px-2 py-1">
                          {key.key_prefix
                            ? `${key.key_prefix}••••••••`
                            : "••••••••••••"}
                        </code>
                      </div>
                    </td>
                    <td className="py-3 font-mono text-xs text-zinc-500">
                      {format(new Date(key.created_at), "MMM d, yyyy")}
                    </td>
                    <td className="py-3">
                      <span
                        className={`font-mono text-xs px-2 py-0.5 border ${
                          key.is_active
                            ? "bg-green-100 dark:bg-green-500/10 text-green-800 dark:text-green-400 border-green-200 dark:border-green-500/20"
                            : "bg-zinc-100 dark:bg-zinc-800 text-zinc-600 dark:text-zinc-400 border-zinc-200 dark:border-zinc-700"
                        }`}
                      >
                        {key.is_active ? "ACTIVE" : "INACTIVE"}
                      </span>
                    </td>
                    <td className="py-3 text-right">
                      <button
                        onClick={() => handleDelete(key.id)}
                        className="p-1 text-red-600 hover:bg-red-100 dark:hover:bg-red-500/10 transition-colors"
                        title="Delete key"
                        data-testid={`delete-key-${key.id}-btn`}
                      >
                        <Trash2 size={16} />
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        <div className="p-4 border-t border-zinc-200 dark:border-zinc-800 bg-zinc-50 dark:bg-zinc-950">
          <p className="font-mono text-xs text-zinc-500 dark:text-zinc-400">
            <AlertTriangle size={12} className="inline mr-1" />
            API keys are only shown once when created. Store them securely.
          </p>
        </div>
      </motion.div>
    </div>
  );
};

// Export Panel
const ExportPanel = ({ isOpen, onClose }) => {
  const [stats, setStats] = useState(null);
  const [loading, setLoading] = useState(false);
  const [source, setSource] = useState("");
  const [availableSources, setAvailableSources] = useState([]);
  const [loadingSources, setLoadingSources] = useState(false);
  const [activeDownload, setActiveDownload] = useState(null); // 'products' | 'results' | 'changes'

  // Reset state and fetch data when modal opens
  useEffect(() => {
    if (isOpen) {
      setSource("");
      setStats(null);

      const fetchSources = async () => {
        setLoadingSources(true);
        try {
          const response = await monitoringAPI.scrapers();
          setAvailableSources(
            response.data.map((s) => ({
              id: s.site_id,
              name: s.name,
            })),
          );
        } catch (err) {
          console.error("Failed to fetch sources:", err);
          setAvailableSources([
            { id: "customnapkinsnow", name: "CustomNapkinsNow" },
            { id: "customcoasters", name: "CustomCoastersNow" },
            { id: "printify", name: "Printify" },
            { id: "yardsignplus", name: "YardSignPlus" },
          ]);
        } finally {
          setLoadingSources(false);
        }
      };
      fetchSources();
      fetchStats("");
    }
  }, [isOpen]);

  const fetchStats = async (sourceFilter) => {
    try {
      const response = await exportAPI.stats({
        source: sourceFilter || undefined,
      });
      setStats(response.data);
    } catch (err) {
      console.error("Failed to fetch export stats:", err);
    }
  };

  const handleSourceChange = (newSource) => {
    setSource(newSource);
    fetchStats(newSource);
  };

  const handleDownload = async (type) => {
    setActiveDownload(type);
    try {
      let response;
      if (type === "products") {
        response = await exportAPI.downloadProductsCsv({
          source: source || undefined,
        });
      } else if (type === "results") {
        response = await exportAPI.downloadResultsCsv({
          source: source || undefined,
        });
      } else if (type === "changes") {
        response = await exportAPI.downloadChangesCsv({
          source: source || undefined,
        });
      }

      // axios returns response.data as a Blob already (responseType: 'blob').
      // Wrapping it in another `new Blob([...])` is unnecessary and would
      // sometimes corrupt the download for streamed CSV; use it directly.
      const url = window.URL.createObjectURL(response.data);
      const a = document.createElement("a");
      a.href = url;
      const sourceLabel = source ? `_${source}` : "";
      a.download = `${type}${sourceLabel}_export_${new Date().toISOString().split("T")[0]}.csv`;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);
    } catch (err) {
      console.error("Failed to download:", err);
    } finally {
      setActiveDownload(null);
    }
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        className="w-full max-w-lg bg-white dark:bg-zinc-900 border border-zinc-200 dark:border-zinc-800"
      >
        <div className="flex items-center justify-between p-4 border-b border-zinc-200 dark:border-zinc-800">
          <h3 className="font-heading text-lg font-bold tracking-tight text-zinc-950 dark:text-white">
            Export Data
          </h3>
          <button
            onClick={onClose}
            className="p-1 hover:bg-zinc-100 dark:hover:bg-zinc-800 transition-colors"
            data-testid="close-export-panel-btn"
          >
            <X size={20} strokeWidth={1.5} />
          </button>
        </div>

        <div className="p-4 space-y-4">
          <div>
            <label className="font-mono text-xs uppercase tracking-[0.15em] text-zinc-600 dark:text-zinc-400 mb-2 block">
              Filter by Source (optional)
            </label>
            <select
              value={source}
              onChange={(e) => handleSourceChange(e.target.value)}
              disabled={loadingSources}
              className="w-full px-4 py-3 font-mono text-sm bg-white dark:bg-zinc-950 border border-zinc-300 dark:border-zinc-700 rounded-none text-zinc-950 dark:text-white focus:outline-none focus:ring-2 focus:ring-black dark:focus:ring-white disabled:opacity-50"
              data-testid="export-source-filter"
            >
              <option value="">
                {loadingSources ? "Loading sources..." : "All Sources"}
              </option>
              {availableSources.map((s) => (
                <option key={s.id} value={s.id}>
                  {s.name}
                </option>
              ))}
            </select>
          </div>

          {stats && (
            <div className="grid grid-cols-2 gap-4">
              <div className="p-4 border border-zinc-200 dark:border-zinc-800 bg-zinc-50 dark:bg-zinc-950">
                <p className="font-mono text-xs text-zinc-500 mb-1">PRODUCTS</p>
                <p className="font-mono text-2xl font-bold text-zinc-950 dark:text-white">
                  {stats.products_collection_count}
                </p>
              </div>
              <div className="p-4 border border-zinc-200 dark:border-zinc-800 bg-zinc-50 dark:bg-zinc-950">
                <p className="font-mono text-xs text-zinc-500 mb-1">RESULTS</p>
                <p className="font-mono text-2xl font-bold text-zinc-950 dark:text-white">
                  {stats.results_collection_count}
                </p>
              </div>
            </div>
          )}

          {/* Product & Results CSVs */}
          <div className="flex gap-3">
            <button
              onClick={() => handleDownload("products")}
              disabled={!!activeDownload}
              className="flex-1 flex items-center justify-center gap-2 py-3 bg-zinc-950 dark:bg-white text-white dark:text-zinc-950 font-mono font-bold text-sm hover:bg-zinc-800 dark:hover:bg-zinc-200 disabled:opacity-50 transition-colors"
              data-testid="download-products-btn"
            >
              {activeDownload === "products" ? (
                <Loader2 size={16} className="animate-spin" />
              ) : (
                <Download size={16} />
              )}
              PRODUCTS CSV
            </button>
            <button
              onClick={() => handleDownload("results")}
              disabled={!!activeDownload}
              className="flex-1 flex items-center justify-center gap-2 py-3 bg-white dark:bg-zinc-950 border border-zinc-200 dark:border-zinc-800 text-zinc-950 dark:text-white font-mono font-medium text-sm hover:bg-zinc-100 dark:hover:bg-zinc-800 disabled:opacity-50 transition-colors"
              data-testid="download-results-btn"
            >
              {activeDownload === "results" ? (
                <Loader2 size={16} className="animate-spin" />
              ) : (
                <Download size={16} />
              )}
              RESULTS CSV
            </button>
          </div>

          {/* Changes CSV — separate section with description */}
          <div className="border-t border-zinc-200 dark:border-zinc-800 pt-4">
            <div className="flex items-start gap-3 mb-3">
              <GitCompare
                size={16}
                className="text-zinc-400 mt-0.5 flex-shrink-0"
              />
              <div>
                <p className="font-mono text-xs font-bold text-zinc-700 dark:text-zinc-300 uppercase tracking-[0.1em]">
                  Change Tracking Log
                </p>
                <p className="font-mono text-xs text-zinc-500 dark:text-zinc-400 mt-1">
                  Audit log of what was added, updated, or deleted between
                  scrape runs — with field-level diffs.
                </p>
              </div>
            </div>
            <button
              onClick={() => handleDownload("changes")}
              disabled={!!activeDownload}
              className="w-full flex items-center justify-center gap-2 py-3 bg-white dark:bg-zinc-950 border border-zinc-200 dark:border-zinc-800 text-zinc-950 dark:text-white font-mono font-medium text-sm hover:bg-zinc-100 dark:hover:bg-zinc-800 disabled:opacity-50 transition-colors"
              data-testid="download-changes-btn"
            >
              {activeDownload === "changes" ? (
                <Loader2 size={16} className="animate-spin" />
              ) : (
                <GitCompare size={16} />
              )}
              CHANGES LOG CSV
            </button>
          </div>
        </div>
      </motion.div>
    </div>
  );
};

// Main Dashboard
const Dashboard = () => {
  const navigate = useNavigate();
  const { user, logout } = useAuth();
  const { isDark, toggleTheme } = useTheme();

  const isAdmin = user?.role === "ADMIN";

  const [activeTab, setActiveTab] = useState("dashboard");
  const [summary, setSummary] = useState(null);
  const [tasks, setTasks] = useState([]);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const [statusFilter, setStatusFilter] = useState("");
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [showKeysPanel, setShowKeysPanel] = useState(false);
  const [showExportPanel, setShowExportPanel] = useState(false);
  const [terminalLogs, setTerminalLogs] = useState([]);
  const [healthStatus, setHealthStatus] = useState(null);
  const [selectedTask, setSelectedTask] = useState(null);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [activeSiteTasks, setActiveSiteTasks] = useState([]);

  // Get list of sites with active (non-terminal) tasks
  const updateActiveSiteTasks = useCallback((tasksList) => {
    const terminalStatuses = ["completed", "failed", "cancelled"];
    const activeSites = [
      ...new Set(
        tasksList
          .filter((t) => !terminalStatuses.includes(t.status))
          .map((t) => t.site),
      ),
    ];
    setActiveSiteTasks(activeSites);
  }, []);

  const fetchSummary = useCallback(async () => {
    try {
      const response = await taskAPI.summary();
      setSummary(response.data);
    } catch (err) {
      console.error("Failed to fetch summary:", err);
    }
  }, []);

  const fetchTasks = useCallback(async () => {
    try {
      const params = { page, page_size: 10 };
      if (statusFilter) params.status = statusFilter;
      const response = await taskAPI.list(params);
      setTasks(response.data.tasks);
      setTotalPages(Math.ceil(response.data.total / 10) || 1);

      // Also fetch all non-terminal tasks to track active sites
      // This ensures we know which sites have running tasks
      const allTasksResponse = await taskAPI.list({ page: 1, page_size: 100 });
      updateActiveSiteTasks(allTasksResponse.data.tasks);
    } catch (err) {
      console.error("Failed to fetch tasks:", err);
    }
  }, [page, statusFilter, updateActiveSiteTasks]);

  const fetchHealth = useCallback(async () => {
    try {
      const response = await monitoringAPI.health();
      setHealthStatus(response.data);
    } catch (err) {
      setHealthStatus({ status: "unhealthy" });
    }
  }, []);

  useEffect(() => {
    const init = async () => {
      setLoading(true);
      await Promise.all([fetchSummary(), fetchTasks(), fetchHealth()]);
      setLoading(false);
    };
    init();

    // No continuous polling - stats are fetched on task creation/cancellation
  }, [fetchSummary, fetchTasks, fetchHealth]);

  useEffect(() => {
    fetchTasks();
  }, [page, statusFilter, fetchTasks]);

  const handleCreateTask = async (data) => {
    try {
      const response = await taskAPI.create(data);
      const newLog = {
        id: Date.now(),
        time: format(new Date(), "HH:mm:ss"),
        type: "success",
        message: `Task created: ${response.data.id} [${data.site}]`,
      };
      setTerminalLogs((prev) => [newLog, ...prev].slice(0, 50));
      fetchSummary();
      fetchTasks();
    } catch (err) {
      const newLog = {
        id: Date.now(),
        time: format(new Date(), "HH:mm:ss"),
        type: "error",
        message: `Failed to create task: ${err.response?.data?.detail || err.message}`,
      };
      setTerminalLogs((prev) => [newLog, ...prev].slice(0, 50));
      throw err;
    }
  };

  const handleCancelTask = async (taskId) => {
    try {
      await taskAPI.cancel(taskId);
      const newLog = {
        id: Date.now(),
        time: format(new Date(), "HH:mm:ss"),
        type: "warning",
        message: `Task cancelled: ${taskId}`,
      };
      setTerminalLogs((prev) => [newLog, ...prev].slice(0, 50));
      fetchSummary();
      fetchTasks();
      if (selectedTask?.id === taskId) {
        const response = await taskAPI.get(taskId);
        setSelectedTask(response.data);
      }
    } catch (err) {
      console.error("Failed to cancel task:", err);
    }
  };

  const handleRefresh = async () => {
    setIsRefreshing(true);
    await Promise.all([fetchSummary(), fetchTasks(), fetchHealth()]);
    setIsRefreshing(false);
  };

  const statusIcons = {
    pending: <Clock size={14} className="text-zinc-500" />,
    queued: <Loader2 size={14} className="text-blue-500 animate-spin" />,
    started: <Loader2 size={14} className="text-blue-500 animate-spin" />,
    running: <Loader2 size={14} className="text-blue-500 animate-spin" />,
    completed: <CheckCircle2 size={14} className="text-green-500" />,
    failed: <XCircle size={14} className="text-red-500" />,
    retrying: <AlertTriangle size={14} className="text-yellow-500" />,
    cancelled: <Pause size={14} className="text-zinc-500" />,
  };

  // Generate chart data from summary
  const chartData = summary
    ? [
        { name: "Pending", value: summary.pending },
        {
          name: "Running",
          value: summary.running + summary.started + summary.queued,
        },
        { name: "Completed", value: summary.completed },
        { name: "Failed", value: summary.failed },
      ]
    : [];

  return (
    <div className="min-h-screen bg-white dark:bg-zinc-950">
      {/* Header */}
      <header className="border-b border-zinc-200 dark:border-zinc-800 sticky top-0 z-40 bg-white dark:bg-zinc-950">
        <div className="max-w-[1800px] mx-auto px-4 md:px-6 py-4 flex items-center justify-between">
          <div className="flex items-center gap-6">
            <h1 className="font-heading text-xl font-black tracking-tighter text-zinc-950 dark:text-white">
              SCRAPER.
            </h1>
            {healthStatus && (
              <div
                className={`flex items-center gap-2 px-3 py-1 border font-mono text-xs ${
                  healthStatus.status === "healthy"
                    ? "bg-green-100 dark:bg-green-500/10 text-green-800 dark:text-green-400 border-green-200 dark:border-green-500/20"
                    : "bg-red-100 dark:bg-red-500/10 text-red-800 dark:text-red-400 border-red-200 dark:border-red-500/20"
                }`}
              >
                <span
                  className={`w-2 h-2 rounded-full ${healthStatus.status === "healthy" ? "bg-green-500" : "bg-red-500"}`}
                />
                {healthStatus.status?.toUpperCase()}
              </div>
            )}
          </div>

          <div className="flex items-center gap-2">
            <button
              onClick={handleRefresh}
              disabled={isRefreshing}
              className="p-2 hover:bg-zinc-100 dark:hover:bg-zinc-800 transition-colors disabled:opacity-50"
              title="Refresh Stats"
              data-testid="refresh-btn"
            >
              <RefreshCw
                size={18}
                strokeWidth={1.5}
                className={`text-zinc-600 dark:text-zinc-400 ${isRefreshing ? "animate-spin" : ""}`}
              />
            </button>
            <button
              onClick={toggleTheme}
              className="p-2 hover:bg-zinc-100 dark:hover:bg-zinc-800 transition-colors"
              title="Toggle theme"
              data-testid="theme-toggle-btn"
            >
              {isDark ? (
                <Sun size={18} strokeWidth={1.5} className="text-zinc-400" />
              ) : (
                <Moon size={18} strokeWidth={1.5} className="text-zinc-600" />
              )}
            </button>
            <div className="w-px h-6 bg-zinc-200 dark:bg-zinc-800 mx-2" />
            <div className="flex items-center gap-3">
              <div className="text-right hidden sm:block">
                <p className="font-mono text-sm text-zinc-950 dark:text-white">
                  {user?.email}
                </p>
                <p className="font-mono text-xs text-zinc-500">{user?.role}</p>
              </div>
              <button
                onClick={logout}
                className="p-2 hover:bg-zinc-100 dark:hover:bg-zinc-800 transition-colors"
                title="Logout"
                data-testid="logout-btn"
              >
                <LogOut
                  size={18}
                  strokeWidth={1.5}
                  className="text-zinc-600 dark:text-zinc-400"
                />
              </button>
            </div>
          </div>
        </div>
      </header>

      {/* Navigation Tabs */}
      <nav className="border-b border-zinc-200 dark:border-zinc-800 bg-zinc-50 dark:bg-zinc-900">
        <div className="max-w-[1800px] mx-auto px-4 md:px-6 flex items-center gap-1">
          {[
            { id: "dashboard", label: "Dashboard", icon: LayoutGrid },
            { id: "tasks", label: "Tasks", icon: Activity },
          ].map(({ id, label, icon: Icon }) => (
            <button
              key={id}
              onClick={() => setActiveTab(id)}
              className={`flex items-center gap-2 px-4 py-3 font-mono text-sm transition-colors border-b-2 -mb-[1px] ${
                activeTab === id
                  ? "border-zinc-950 dark:border-white text-zinc-950 dark:text-white"
                  : "border-transparent text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300"
              }`}
              data-testid={`nav-${id}-btn`}
            >
              <Icon size={16} strokeWidth={1.5} />
              {label}
            </button>
          ))}
          <div className="flex-1" />
          <button
            onClick={() => setShowKeysPanel(true)}
            className="flex items-center gap-2 px-4 py-3 font-mono text-sm text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300 transition-colors"
            data-testid="api-keys-btn"
          >
            <Key size={16} strokeWidth={1.5} />
            API Keys
          </button>
          <button
            onClick={() => setShowExportPanel(true)}
            className="flex items-center gap-2 px-4 py-3 font-mono text-sm text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300 transition-colors"
            data-testid="export-btn"
          >
            <Download size={16} strokeWidth={1.5} />
            Export
          </button>
          {isAdmin && (
            <button
              onClick={() => navigate("/activity")}
              className="flex items-center gap-2 px-4 py-3 font-mono text-sm text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300 transition-colors"
              data-testid="activity-monitor-btn"
            >
              <Users size={16} strokeWidth={1.5} />
              Activity
            </button>
          )}
        </div>
      </nav>

      {/* Main Content */}
      <main className="max-w-[1800px] mx-auto p-4 md:p-6">
        {loading ? (
          <div className="flex items-center justify-center py-20">
            <div className="flex items-center gap-3">
              <Loader2 size={20} className="animate-spin text-zinc-500" />
              <span className="font-mono text-zinc-500">
                Loading dashboard...
              </span>
            </div>
          </div>
        ) : activeTab === "dashboard" ? (
          <div className="space-y-6">
            {/* Metrics Header with Refresh Button */}
            <div className="flex items-center justify-between">
              <p className="font-mono text-xs uppercase tracking-[0.2em] text-zinc-500 dark:text-zinc-400">
                OVERVIEW
              </p>
              <button
                onClick={handleRefresh}
                disabled={isRefreshing}
                className="flex items-center gap-2 px-3 py-1.5 text-zinc-600 dark:text-zinc-400 hover:text-zinc-950 dark:hover:text-white hover:bg-zinc-100 dark:hover:bg-zinc-800 transition-colors font-mono text-xs disabled:opacity-50"
                data-testid="refresh-stats-btn"
              >
                <RefreshCw
                  size={14}
                  className={isRefreshing ? "animate-spin" : ""}
                />
                {isRefreshing ? "REFRESHING..." : "REFRESH STATS"}
              </button>
            </div>

            {/* Metrics Grid */}
            <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-4">
              <MetricCard
                label="Total Tasks"
                value={summary?.total || 0}
                icon={Activity}
              />
              <MetricCard
                label="Running"
                value={
                  (summary?.running || 0) +
                  (summary?.started || 0) +
                  (summary?.queued || 0)
                }
                icon={Loader2}
                color="info"
              />
              <MetricCard
                label="Completed"
                value={summary?.completed || 0}
                icon={CheckCircle2}
                color="success"
              />
              <MetricCard
                label="Failed"
                value={summary?.failed || 0}
                icon={XCircle}
                color="danger"
              />
              <MetricCard
                label="Success Rate"
                value={
                  summary?.success_rate
                    ? `${summary.success_rate.toFixed(1)}%`
                    : "N/A"
                }
                color="success"
              />
              <MetricCard
                label="Avg Duration"
                value={
                  summary?.avg_duration_ms
                    ? `${(summary.avg_duration_ms / 1000).toFixed(1)}s`
                    : "N/A"
                }
                icon={Clock}
              />
            </div>

            {/* Chart + Actions */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
              {/* Chart */}
              <div className="lg:col-span-2 border border-zinc-200 dark:border-zinc-800 bg-zinc-50 dark:bg-zinc-900 p-4 md:p-6">
                <p className="font-mono text-xs uppercase tracking-[0.2em] text-zinc-500 dark:text-zinc-400 mb-4">
                  TASK DISTRIBUTION
                </p>
                <div className="h-48">
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={chartData}>
                      <XAxis
                        dataKey="name"
                        axisLine={false}
                        tickLine={false}
                        tick={{
                          fill: "#71717a",
                          fontSize: 12,
                          fontFamily: "IBM Plex Mono",
                        }}
                      />
                      <YAxis
                        axisLine={false}
                        tickLine={false}
                        tick={{
                          fill: "#71717a",
                          fontSize: 12,
                          fontFamily: "IBM Plex Mono",
                        }}
                      />
                      <Tooltip
                        contentStyle={{
                          backgroundColor: isDark ? "#18181b" : "#fff",
                          border: `1px solid ${isDark ? "#27272a" : "#e4e4e7"}`,
                          fontFamily: "IBM Plex Mono",
                          fontSize: 12,
                        }}
                      />
                      <Line
                        type="monotone"
                        dataKey="value"
                        stroke={isDark ? "#fff" : "#18181b"}
                        strokeWidth={2}
                        dot={{
                          fill: isDark ? "#fff" : "#18181b",
                          strokeWidth: 0,
                        }}
                      />
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              </div>

              {/* Quick Actions */}
              <div className="border border-zinc-200 dark:border-zinc-800 bg-zinc-50 dark:bg-zinc-900 p-4 md:p-6">
                <p className="font-mono text-xs uppercase tracking-[0.2em] text-zinc-500 dark:text-zinc-400 mb-4">
                  QUICK ACTIONS
                </p>
                <div className="space-y-3">
                  <button
                    onClick={() => setShowCreateModal(true)}
                    className="w-full flex items-center justify-center gap-2 py-3 bg-zinc-950 dark:bg-white text-white dark:text-zinc-950 font-mono font-bold text-sm hover:bg-zinc-800 dark:hover:bg-zinc-200 transition-colors"
                    data-testid="create-task-btn"
                  >
                    <Plus size={16} />
                    NEW SCRAPING TASK
                  </button>
                  <button
                    onClick={() => setShowExportPanel(true)}
                    className="w-full flex items-center justify-center gap-2 py-3 bg-white dark:bg-zinc-950 border border-zinc-200 dark:border-zinc-800 text-zinc-950 dark:text-white font-mono font-medium text-sm hover:bg-zinc-100 dark:hover:bg-zinc-800 transition-colors"
                    data-testid="export-data-btn"
                  >
                    <Download size={16} />
                    EXPORT DATA
                  </button>
                </div>

                {/* Status Summary */}
                <div className="mt-6 space-y-2">
                  <p className="font-mono text-xs uppercase tracking-[0.2em] text-zinc-500 dark:text-zinc-400">
                    BY STATUS
                  </p>
                  {[
                    "pending",
                    "queued",
                    "running",
                    "completed",
                    "failed",
                    "retrying",
                    "cancelled",
                  ].map((status) => (
                    <div
                      key={status}
                      className="flex items-center justify-between py-1"
                    >
                      <span className="flex items-center gap-2">
                        {statusIcons[status]}
                        <span className="font-mono text-xs text-zinc-600 dark:text-zinc-400 uppercase">
                          {status}
                        </span>
                      </span>
                      <span className="font-mono text-sm font-bold text-zinc-950 dark:text-white">
                        {summary?.[status] || 0}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            </div>

            {/* Terminal Feed */}
            <div className="border border-zinc-200 dark:border-zinc-800">
              <div className="flex items-center gap-2 px-4 py-2 border-b border-zinc-200 dark:border-zinc-800 bg-zinc-50 dark:bg-zinc-900">
                <Terminal size={16} className="text-zinc-500" />
                <span className="font-mono text-xs uppercase tracking-[0.2em] text-zinc-500">
                  ACTIVITY FEED
                </span>
              </div>
              <TerminalLog logs={terminalLogs} />
            </div>
          </div>
        ) : (
          /* Tasks Tab */
          <div className="space-y-6">
            {/* Tasks Header */}
            <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
              <div className="flex items-center gap-4">
                <h2 className="font-heading text-2xl font-bold tracking-tight text-zinc-950 dark:text-white">
                  Tasks
                </h2>
                <div className="flex items-center gap-2">
                  <Filter size={16} className="text-zinc-400" />
                  <select
                    value={statusFilter}
                    onChange={(e) => {
                      setStatusFilter(e.target.value);
                      setPage(1);
                    }}
                    className="font-mono text-sm bg-white dark:bg-zinc-950 border border-zinc-300 dark:border-zinc-700 rounded-none px-3 py-1 text-zinc-950 dark:text-white focus:outline-none focus:ring-2 focus:ring-black dark:focus:ring-white"
                    data-testid="status-filter-select"
                  >
                    <option value="">All Statuses</option>
                    <option value="pending">Pending</option>
                    <option value="queued">Queued</option>
                    <option value="running">Running</option>
                    <option value="completed">Completed</option>
                    <option value="failed">Failed</option>
                    <option value="cancelled">Cancelled</option>
                  </select>
                </div>
              </div>
              <button
                onClick={() => setShowCreateModal(true)}
                className="flex items-center justify-center gap-2 px-4 py-2 bg-zinc-950 dark:bg-white text-white dark:text-zinc-950 font-mono font-bold text-sm hover:bg-zinc-800 dark:hover:bg-zinc-200 transition-colors"
                data-testid="create-task-top-btn"
              >
                <Plus size={16} />
                NEW TASK
              </button>
            </div>

            {/* Tasks Table */}
            <div className="border border-zinc-200 dark:border-zinc-800 overflow-x-auto">
              <table className="w-full text-sm">
                <thead className="bg-zinc-50 dark:bg-zinc-900 border-b border-zinc-200 dark:border-zinc-800">
                  <tr>
                    <th className="text-left font-mono text-xs uppercase tracking-[0.15em] text-zinc-500 px-4 py-3">
                      ID
                    </th>
                    <th className="text-left font-mono text-xs uppercase tracking-[0.15em] text-zinc-500 px-4 py-3">
                      Site
                    </th>
                    <th className="text-left font-mono text-xs uppercase tracking-[0.15em] text-zinc-500 px-4 py-3">
                      Status
                    </th>
                    <th className="text-left font-mono text-xs uppercase tracking-[0.15em] text-zinc-500 px-4 py-3">
                      Created
                    </th>
                    <th className="text-left font-mono text-xs uppercase tracking-[0.15em] text-zinc-500 px-4 py-3">
                      Duration
                    </th>
                    <th className="text-right font-mono text-xs uppercase tracking-[0.15em] text-zinc-500 px-4 py-3">
                      Actions
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {tasks.length === 0 ? (
                    <tr>
                      <td
                        colSpan={6}
                        className="px-4 py-12 text-center font-mono text-zinc-500"
                      >
                        No tasks found
                      </td>
                    </tr>
                  ) : (
                    tasks.map((task) => (
                      <tr
                        key={task.id}
                        className="border-b border-zinc-100 dark:border-zinc-800/50 hover:bg-zinc-50 dark:hover:bg-zinc-900/50 cursor-pointer transition-colors"
                        onClick={() => setSelectedTask(task)}
                        data-testid={`task-row-${task.id}`}
                      >
                        <td className="px-4 py-3">
                          <code className="font-mono text-xs text-zinc-600 dark:text-zinc-400">
                            {task.id.slice(0, 8)}...
                          </code>
                        </td>
                        <td className="px-4 py-3 font-mono text-zinc-950 dark:text-white">
                          {task.site}
                        </td>
                        <td className="px-4 py-3">
                          <StatusBadge status={task.status} />
                        </td>
                        <td className="px-4 py-3 font-mono text-xs text-zinc-500">
                          {formatDistanceToNow(new Date(task.created_at), {
                            addSuffix: true,
                          })}
                        </td>
                        <td className="px-4 py-3 font-mono text-xs text-zinc-500">
                          {task.duration_ms
                            ? `${(task.duration_ms / 1000).toFixed(2)}s`
                            : "-"}
                        </td>
                        <td className="px-4 py-3 text-right">
                          <div className="flex items-center justify-end gap-2">
                            <button
                              onClick={(e) => {
                                e.stopPropagation();
                                setSelectedTask(task);
                              }}
                              className="p-1 hover:bg-zinc-100 dark:hover:bg-zinc-800 transition-colors"
                              title="View details"
                              data-testid={`view-task-${task.id}-btn`}
                            >
                              <ExternalLink
                                size={14}
                                className="text-zinc-400"
                              />
                            </button>
                            {!["completed", "failed", "cancelled"].includes(
                              task.status,
                            ) && (
                              <button
                                onClick={(e) => {
                                  e.stopPropagation();
                                  handleCancelTask(task.id);
                                }}
                                className="p-1 hover:bg-red-100 dark:hover:bg-red-500/10 text-red-500 transition-colors"
                                title="Cancel task"
                                data-testid={`cancel-task-${task.id}-btn`}
                              >
                                <X size={14} />
                              </button>
                            )}
                          </div>
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>

            {/* Pagination */}
            <div className="flex items-center justify-between">
              <p className="font-mono text-sm text-zinc-500">
                Page {page} of {totalPages}
              </p>
              <div className="flex items-center gap-2">
                <button
                  onClick={() => setPage((p) => Math.max(1, p - 1))}
                  disabled={page === 1}
                  className="p-2 hover:bg-zinc-100 dark:hover:bg-zinc-800 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                  data-testid="prev-page-btn"
                >
                  <ChevronLeft
                    size={18}
                    className="text-zinc-600 dark:text-zinc-400"
                  />
                </button>
                <button
                  onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
                  disabled={page === totalPages}
                  className="p-2 hover:bg-zinc-100 dark:hover:bg-zinc-800 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                  data-testid="next-page-btn"
                >
                  <ChevronRight
                    size={18}
                    className="text-zinc-600 dark:text-zinc-400"
                  />
                </button>
              </div>
            </div>
          </div>
        )}
      </main>

      {/* Task Detail Sidebar */}
      <AnimatePresence>
        {selectedTask && (
          <>
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              className="fixed inset-0 bg-black/30 z-40"
              onClick={() => setSelectedTask(null)}
            />
            <motion.div
              initial={{ x: "100%" }}
              animate={{ x: 0 }}
              exit={{ x: "100%" }}
              className="fixed right-0 top-0 bottom-0 w-full max-w-lg bg-white dark:bg-zinc-900 border-l border-zinc-200 dark:border-zinc-800 z-50 overflow-auto"
            >
              <div className="p-6 border-b border-zinc-200 dark:border-zinc-800 flex items-center justify-between sticky top-0 bg-white dark:bg-zinc-900">
                <h3 className="font-heading text-lg font-bold tracking-tight text-zinc-950 dark:text-white">
                  Task Details
                </h3>
                <button
                  onClick={() => setSelectedTask(null)}
                  className="p-1 hover:bg-zinc-100 dark:hover:bg-zinc-800 transition-colors"
                  data-testid="close-task-detail-btn"
                >
                  <X size={20} strokeWidth={1.5} />
                </button>
              </div>

              <div className="p-6 space-y-6">
                <div>
                  <p className="font-mono text-xs uppercase tracking-[0.15em] text-zinc-500 mb-2">
                    TASK ID
                  </p>
                  <code className="font-mono text-sm text-zinc-950 dark:text-white break-all">
                    {selectedTask.id}
                  </code>
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <p className="font-mono text-xs uppercase tracking-[0.15em] text-zinc-500 mb-2">
                      SITE
                    </p>
                    <p className="font-mono text-sm text-zinc-950 dark:text-white">
                      {selectedTask.site}
                    </p>
                  </div>
                  <div>
                    <p className="font-mono text-xs uppercase tracking-[0.15em] text-zinc-500 mb-2">
                      STATUS
                    </p>
                    <StatusBadge status={selectedTask.status} />
                  </div>
                </div>

                <div>
                  <p className="font-mono text-xs uppercase tracking-[0.15em] text-zinc-500 mb-2">
                    URL
                  </p>
                  <a
                    href={selectedTask.url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="font-mono text-sm text-blue-600 dark:text-blue-400 hover:underline break-all"
                  >
                    {selectedTask.url}
                  </a>
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <p className="font-mono text-xs uppercase tracking-[0.15em] text-zinc-500 mb-2">
                      RETRIES
                    </p>
                    <p className="font-mono text-sm text-zinc-950 dark:text-white">
                      {selectedTask.retries_attempted} /{" "}
                      {selectedTask.max_retries}
                    </p>
                  </div>
                </div>

                {selectedTask.duration_ms && (
                  <div>
                    <p className="font-mono text-xs uppercase tracking-[0.15em] text-zinc-500 mb-2">
                      DURATION
                    </p>
                    <p className="font-mono text-sm text-zinc-950 dark:text-white">
                      {(selectedTask.duration_ms / 1000).toFixed(2)} seconds
                    </p>
                  </div>
                )}

                {selectedTask.error_message && (
                  <div>
                    <p className="font-mono text-xs uppercase tracking-[0.15em] text-red-500 mb-2">
                      ERROR
                    </p>
                    <p className="font-mono text-sm text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-500/10 p-3 border border-red-200 dark:border-red-500/20">
                      {selectedTask.error_message}
                    </p>
                  </div>
                )}

                {selectedTask.tags?.length > 0 && (
                  <div>
                    <p className="font-mono text-xs uppercase tracking-[0.15em] text-zinc-500 mb-2">
                      TAGS
                    </p>
                    <div className="flex flex-wrap gap-2">
                      {selectedTask.tags.map((tag, idx) => (
                        <span
                          key={idx}
                          className="font-mono text-xs px-2 py-1 bg-zinc-100 dark:bg-zinc-800 text-zinc-600 dark:text-zinc-400 border border-zinc-200 dark:border-zinc-700"
                        >
                          {tag}
                        </span>
                      ))}
                    </div>
                  </div>
                )}

                <div className="space-y-2 pt-4 border-t border-zinc-200 dark:border-zinc-800">
                  <p className="font-mono text-xs uppercase tracking-[0.15em] text-zinc-500 mb-2">
                    TIMELINE
                  </p>
                  <div className="space-y-2 text-sm font-mono">
                    <div className="flex justify-between">
                      <span className="text-zinc-500">Created</span>
                      <span className="text-zinc-950 dark:text-white">
                        {format(
                          new Date(selectedTask.created_at),
                          "MMM d, yyyy HH:mm:ss",
                        )}
                      </span>
                    </div>
                    {selectedTask.queued_at && (
                      <div className="flex justify-between">
                        <span className="text-zinc-500">Queued</span>
                        <span className="text-zinc-950 dark:text-white">
                          {format(
                            new Date(selectedTask.queued_at),
                            "MMM d, yyyy HH:mm:ss",
                          )}
                        </span>
                      </div>
                    )}
                    {selectedTask.started_at && (
                      <div className="flex justify-between">
                        <span className="text-zinc-500">Started</span>
                        <span className="text-zinc-950 dark:text-white">
                          {format(
                            new Date(selectedTask.started_at),
                            "MMM d, yyyy HH:mm:ss",
                          )}
                        </span>
                      </div>
                    )}
                    {selectedTask.completed_at && (
                      <div className="flex justify-between">
                        <span className="text-zinc-500">Completed</span>
                        <span className="text-zinc-950 dark:text-white">
                          {format(
                            new Date(selectedTask.completed_at),
                            "MMM d, yyyy HH:mm:ss",
                          )}
                        </span>
                      </div>
                    )}
                  </div>
                </div>

                {!["completed", "failed", "cancelled"].includes(
                  selectedTask.status,
                ) && (
                  <button
                    onClick={() => handleCancelTask(selectedTask.id)}
                    className="w-full flex items-center justify-center gap-2 py-3 bg-red-100 dark:bg-red-500/10 text-red-800 dark:text-red-400 border border-red-200 dark:border-red-500/20 font-mono font-bold text-sm hover:bg-red-200 dark:hover:bg-red-500/20 transition-colors"
                    data-testid="cancel-task-detail-btn"
                  >
                    <X size={16} />
                    CANCEL TASK
                  </button>
                )}
              </div>
            </motion.div>
          </>
        )}
      </AnimatePresence>

      {/* Modals */}
      <CreateTaskModal
        isOpen={showCreateModal}
        onClose={() => setShowCreateModal(false)}
        onSubmit={handleCreateTask}
        activeSiteTasks={activeSiteTasks}
      />
      <ApiKeysPanel
        isOpen={showKeysPanel}
        onClose={() => setShowKeysPanel(false)}
      />
      <ExportPanel
        isOpen={showExportPanel}
        onClose={() => setShowExportPanel(false)}
      />
    </div>
  );
};

export default Dashboard;
