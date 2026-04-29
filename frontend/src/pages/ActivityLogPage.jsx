import { useState, useEffect, useCallback } from "react";
import { motion } from "framer-motion";
import {
  Activity,
  ArrowLeft,
  Search,
  Filter,
  ChevronLeft,
  ChevronRight,
  LogIn,
  LogOut,
  AlertCircle,
  CheckCircle,
  XCircle,
  Users,
  Calendar,
  RefreshCw,
} from "lucide-react";
import { useNavigate } from "react-router-dom";
import { useAuth } from "../context/AuthContext";
import { activityAPI } from "../utils/api";
import { format } from "date-fns";

const ActivityLogPage = () => {
  const navigate = useNavigate();
  const { user } = useAuth();
  const [logs, setLogs] = useState([]);
  const [stats, setStats] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // Pagination
  const [page, setPage] = useState(1);
  const [pageSize] = useState(50);
  const [totalPages, setTotalPages] = useState(1);
  const [total, setTotal] = useState(0);

  // Filters
  const [userEmailFilter, setUserEmailFilter] = useState("");
  const [activityTypeFilter, setActivityTypeFilter] = useState("");
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [showFilters, setShowFilters] = useState(false);

  // Check if user is admin
  const isAdmin = user?.role === "ADMIN";

  const fetchLogs = useCallback(async () => {
    if (!isAdmin) return;

    setLoading(true);
    setError(null);

    try {
      const params = {
        page,
        page_size: pageSize,
      };

      if (userEmailFilter) params.user_email = userEmailFilter;
      if (activityTypeFilter) params.activity_type = activityTypeFilter;
      if (startDate) params.start_date = new Date(startDate).toISOString();
      if (endDate) params.end_date = new Date(endDate).toISOString();

      const response = await activityAPI.getLogs(params);
      setLogs(response.data.items);
      setTotalPages(response.data.total_pages);
      setTotal(response.data.total);
    } catch (err) {
      console.error("Failed to fetch activity logs:", err);
      setError("Failed to load activity logs");
    } finally {
      setLoading(false);
    }
  }, [
    isAdmin,
    page,
    pageSize,
    userEmailFilter,
    activityTypeFilter,
    startDate,
    endDate,
  ]);

  const fetchStats = useCallback(async () => {
    if (!isAdmin) return;

    try {
      const response = await activityAPI.getStats();
      setStats(response.data);
    } catch (err) {
      console.error("Failed to fetch activity stats:", err);
    }
  }, [isAdmin]);

  useEffect(() => {
    fetchLogs();
    fetchStats();
  }, [fetchLogs, fetchStats]);

  const handleSearch = (e) => {
    e.preventDefault();
    setPage(1);
    fetchLogs();
  };

  const clearFilters = () => {
    setUserEmailFilter("");
    setActivityTypeFilter("");
    setStartDate("");
    setEndDate("");
    setPage(1);
  };

  const getActivityIcon = (type, success) => {
    if (type === "LOGIN") {
      return success ? (
        <LogIn className="text-emerald-500" size={18} />
      ) : (
        <LogIn className="text-red-500" size={18} />
      );
    }
    return <LogOut className="text-amber-500" size={18} />;
  };

  const getActivityBadge = (type, success) => {
    if (type === "LOGIN" && success) {
      return (
        <span className="px-2 py-1 text-xs font-mono bg-emerald-100 dark:bg-emerald-500/20 text-emerald-700 dark:text-emerald-400 rounded">
          LOGIN
        </span>
      );
    }
    if (type === "LOGIN" && !success) {
      return (
        <span className="px-2 py-1 text-xs font-mono bg-red-100 dark:bg-red-500/20 text-red-700 dark:text-red-400 rounded">
          FAILED LOGIN
        </span>
      );
    }
    return (
      <span className="px-2 py-1 text-xs font-mono bg-amber-100 dark:bg-amber-500/20 text-amber-700 dark:text-amber-400 rounded">
        LOGOUT
      </span>
    );
  };

  if (!isAdmin) {
    return (
      <div className="min-h-screen bg-white dark:bg-zinc-950 flex items-center justify-center">
        <div className="text-center">
          <AlertCircle className="mx-auto text-red-500 mb-4" size={48} />
          <h1 className="font-heading text-2xl font-bold text-zinc-950 dark:text-white mb-2">
            Access Denied
          </h1>
          <p className="font-mono text-sm text-zinc-500 dark:text-zinc-400 mb-6">
            You need admin privileges to view this page.
          </p>
          <button
            onClick={() => navigate("/")}
            className="px-6 py-3 bg-zinc-950 dark:bg-white text-white dark:text-zinc-950 font-mono text-sm hover:bg-zinc-800 dark:hover:bg-zinc-200 transition-colors"
          >
            Go to Dashboard
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-zinc-50 dark:bg-zinc-950">
      {/* Header */}
      <header className="bg-white dark:bg-zinc-900 border-b border-zinc-200 dark:border-zinc-800">
        <div className="max-w-7xl mx-auto px-6 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-4">
              <button
                onClick={() => navigate("/")}
                className="p-2 hover:bg-zinc-100 dark:hover:bg-zinc-800 rounded transition-colors"
                data-testid="back-to-dashboard"
              >
                <ArrowLeft
                  size={20}
                  className="text-zinc-600 dark:text-zinc-400"
                />
              </button>
              <div>
                <h1 className="font-heading text-xl font-bold text-zinc-950 dark:text-white flex items-center gap-2">
                  <Activity size={24} />
                  Activity Monitor
                </h1>
                <p className="font-mono text-xs text-zinc-500 dark:text-zinc-400">
                  User login and logout activity
                </p>
              </div>
            </div>
            <button
              onClick={() => {
                fetchLogs();
                fetchStats();
              }}
              className="p-2 hover:bg-zinc-100 dark:hover:bg-zinc-800 rounded transition-colors"
              data-testid="refresh-logs"
            >
              <RefreshCw
                size={20}
                className={`text-zinc-600 dark:text-zinc-400 ${loading ? "animate-spin" : ""}`}
              />
            </button>
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-6 py-8">
        {/* Stats Cards */}
        {stats && (
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-8">
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              className="bg-white dark:bg-zinc-900 border border-zinc-200 dark:border-zinc-800 p-6"
              data-testid="stat-today-logins"
            >
              <div className="flex items-center gap-3 mb-2">
                <CheckCircle className="text-emerald-500" size={20} />
                <span className="font-mono text-xs uppercase tracking-wider text-zinc-500 dark:text-zinc-400">
                  Today's Logins
                </span>
              </div>
              <p className="font-heading text-3xl font-bold text-zinc-950 dark:text-white">
                {stats.today_logins}
              </p>
            </motion.div>

            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.1 }}
              className="bg-white dark:bg-zinc-900 border border-zinc-200 dark:border-zinc-800 p-6"
              data-testid="stat-failed-logins"
            >
              <div className="flex items-center gap-3 mb-2">
                <XCircle className="text-red-500" size={20} />
                <span className="font-mono text-xs uppercase tracking-wider text-zinc-500 dark:text-zinc-400">
                  Failed Attempts
                </span>
              </div>
              <p className="font-heading text-3xl font-bold text-zinc-950 dark:text-white">
                {stats.today_failed_logins}
              </p>
            </motion.div>

            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.2 }}
              className="bg-white dark:bg-zinc-900 border border-zinc-200 dark:border-zinc-800 p-6"
              data-testid="stat-week-logins"
            >
              <div className="flex items-center gap-3 mb-2">
                <Calendar className="text-blue-500" size={20} />
                <span className="font-mono text-xs uppercase tracking-wider text-zinc-500 dark:text-zinc-400">
                  Week's Logins
                </span>
              </div>
              <p className="font-heading text-3xl font-bold text-zinc-950 dark:text-white">
                {stats.week_logins}
              </p>
            </motion.div>

            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.3 }}
              className="bg-white dark:bg-zinc-900 border border-zinc-200 dark:border-zinc-800 p-6"
              data-testid="stat-unique-users"
            >
              <div className="flex items-center gap-3 mb-2">
                <Users className="text-purple-500" size={20} />
                <span className="font-mono text-xs uppercase tracking-wider text-zinc-500 dark:text-zinc-400">
                  Active Users Today
                </span>
              </div>
              <p className="font-heading text-3xl font-bold text-zinc-950 dark:text-white">
                {stats.unique_users_today}
              </p>
            </motion.div>
          </div>
        )}

        {/* Filters */}
        <div className="bg-white dark:bg-zinc-900 border border-zinc-200 dark:border-zinc-800 mb-6">
          <div className="p-4 border-b border-zinc-200 dark:border-zinc-800 flex items-center justify-between">
            <div className="flex items-center gap-2">
              <Filter size={18} className="text-zinc-500" />
              <span className="font-mono text-sm text-zinc-700 dark:text-zinc-300">
                Filters
              </span>
            </div>
            <button
              onClick={() => setShowFilters(!showFilters)}
              className="font-mono text-xs text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300"
            >
              {showFilters ? "Hide" : "Show"}
            </button>
          </div>

          {showFilters && (
            <form
              onSubmit={handleSearch}
              className="p-4 grid grid-cols-1 md:grid-cols-5 gap-4"
            >
              <div>
                <label className="font-mono text-xs uppercase tracking-wider text-zinc-500 dark:text-zinc-400 block mb-2">
                  User Email
                </label>
                <div className="relative">
                  <Search
                    className="absolute left-3 top-1/2 -translate-y-1/2 text-zinc-400"
                    size={16}
                  />
                  <input
                    type="text"
                    value={userEmailFilter}
                    onChange={(e) => setUserEmailFilter(e.target.value)}
                    placeholder="Search email..."
                    className="w-full pl-10 pr-4 py-2 font-mono text-sm bg-white dark:bg-zinc-800 border border-zinc-300 dark:border-zinc-700 text-zinc-950 dark:text-white placeholder-zinc-400 focus:outline-none focus:ring-2 focus:ring-zinc-950 dark:focus:ring-white"
                    data-testid="filter-email"
                  />
                </div>
              </div>

              <div>
                <label className="font-mono text-xs uppercase tracking-wider text-zinc-500 dark:text-zinc-400 block mb-2">
                  Activity Type
                </label>
                <select
                  value={activityTypeFilter}
                  onChange={(e) => setActivityTypeFilter(e.target.value)}
                  className="w-full px-4 py-2 font-mono text-sm bg-white dark:bg-zinc-800 border border-zinc-300 dark:border-zinc-700 text-zinc-950 dark:text-white focus:outline-none focus:ring-2 focus:ring-zinc-950 dark:focus:ring-white"
                  data-testid="filter-type"
                >
                  <option value="">All Types</option>
                  <option value="LOGIN">Login</option>
                  <option value="LOGOUT">Logout</option>
                </select>
              </div>

              <div>
                <label className="font-mono text-xs uppercase tracking-wider text-zinc-500 dark:text-zinc-400 block mb-2">
                  Start Date
                </label>
                <input
                  type="date"
                  value={startDate}
                  onChange={(e) => setStartDate(e.target.value)}
                  className="w-full px-4 py-2 font-mono text-sm bg-white dark:bg-zinc-800 border border-zinc-300 dark:border-zinc-700 text-zinc-950 dark:text-white focus:outline-none focus:ring-2 focus:ring-zinc-950 dark:focus:ring-white"
                  data-testid="filter-start-date"
                />
              </div>

              <div>
                <label className="font-mono text-xs uppercase tracking-wider text-zinc-500 dark:text-zinc-400 block mb-2">
                  End Date
                </label>
                <input
                  type="date"
                  value={endDate}
                  onChange={(e) => setEndDate(e.target.value)}
                  className="w-full px-4 py-2 font-mono text-sm bg-white dark:bg-zinc-800 border border-zinc-300 dark:border-zinc-700 text-zinc-950 dark:text-white focus:outline-none focus:ring-2 focus:ring-zinc-950 dark:focus:ring-white"
                  data-testid="filter-end-date"
                />
              </div>

              <div className="flex items-end gap-2">
                <button
                  type="submit"
                  className="flex-1 px-4 py-2 bg-zinc-950 dark:bg-white text-white dark:text-zinc-950 font-mono text-sm hover:bg-zinc-800 dark:hover:bg-zinc-200 transition-colors"
                  data-testid="apply-filters"
                >
                  Apply
                </button>
                <button
                  type="button"
                  onClick={clearFilters}
                  className="px-4 py-2 border border-zinc-300 dark:border-zinc-700 text-zinc-700 dark:text-zinc-300 font-mono text-sm hover:bg-zinc-100 dark:hover:bg-zinc-800 transition-colors"
                  data-testid="clear-filters"
                >
                  Clear
                </button>
              </div>
            </form>
          )}
        </div>

        {/* Activity Logs Table */}
        <div className="bg-white dark:bg-zinc-900 border border-zinc-200 dark:border-zinc-800">
          <div className="p-4 border-b border-zinc-200 dark:border-zinc-800 flex items-center justify-between">
            <span className="font-mono text-sm text-zinc-700 dark:text-zinc-300">
              {total} total records
            </span>
            <span className="font-mono text-xs text-zinc-500 dark:text-zinc-400">
              Page {page} of {totalPages}
            </span>
          </div>

          {error && (
            <div className="p-4 bg-red-100 dark:bg-red-500/10 text-red-700 dark:text-red-400 font-mono text-sm">
              {error}
            </div>
          )}

          {loading ? (
            <div className="p-8 text-center">
              <div className="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-zinc-950 dark:border-white"></div>
              <p className="mt-4 font-mono text-sm text-zinc-500">
                Loading activity logs...
              </p>
            </div>
          ) : logs.length === 0 ? (
            <div className="p-8 text-center">
              <Activity className="mx-auto text-zinc-400 mb-4" size={48} />
              <p className="font-mono text-sm text-zinc-500">
                No activity logs found
              </p>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full" data-testid="activity-logs-table">
                <thead>
                  <tr className="bg-zinc-50 dark:bg-zinc-800/50">
                    <th className="px-4 py-3 text-left font-mono text-xs uppercase tracking-wider text-zinc-500 dark:text-zinc-400">
                      Activity
                    </th>
                    <th className="px-4 py-3 text-left font-mono text-xs uppercase tracking-wider text-zinc-500 dark:text-zinc-400">
                      User
                    </th>
                    <th className="px-4 py-3 text-left font-mono text-xs uppercase tracking-wider text-zinc-500 dark:text-zinc-400">
                      IP Address
                    </th>
                    <th className="px-4 py-3 text-left font-mono text-xs uppercase tracking-wider text-zinc-500 dark:text-zinc-400">
                      User Agent
                    </th>
                    <th className="px-4 py-3 text-left font-mono text-xs uppercase tracking-wider text-zinc-500 dark:text-zinc-400">
                      Timestamp
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-zinc-200 dark:divide-zinc-800">
                  {logs.map((log) => (
                    <motion.tr
                      key={log.id}
                      initial={{ opacity: 0 }}
                      animate={{ opacity: 1 }}
                      className="hover:bg-zinc-50 dark:hover:bg-zinc-800/50 transition-colors"
                      data-testid={`activity-row-${log.id}`}
                    >
                      <td className="px-4 py-4">
                        <div className="flex items-center gap-3">
                          {getActivityIcon(log.activity_type, log.success)}
                          {getActivityBadge(log.activity_type, log.success)}
                        </div>
                      </td>
                      <td className="px-4 py-4">
                        <span className="font-mono text-sm text-zinc-950 dark:text-white">
                          {log.user_email}
                        </span>
                      </td>
                      <td className="px-4 py-4">
                        <span className="font-mono text-sm text-zinc-600 dark:text-zinc-400">
                          {log.ip_address || "-"}
                        </span>
                      </td>
                      <td className="px-4 py-4 max-w-xs">
                        <span
                          className="font-mono text-xs text-zinc-500 dark:text-zinc-500 truncate block"
                          title={log.user_agent}
                        >
                          {log.user_agent
                            ? log.user_agent.length > 50
                              ? log.user_agent.substring(0, 50) + "..."
                              : log.user_agent
                            : "-"}
                        </span>
                      </td>
                      <td className="px-4 py-4">
                        <span className="font-mono text-sm text-zinc-600 dark:text-zinc-400">
                          {format(
                            new Date(log.created_at),
                            "MMM d, yyyy HH:mm:ss",
                          )}
                        </span>
                      </td>
                    </motion.tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {/* Pagination */}
          {totalPages > 1 && (
            <div className="p-4 border-t border-zinc-200 dark:border-zinc-800 flex items-center justify-between">
              <button
                onClick={() => setPage(Math.max(1, page - 1))}
                disabled={page === 1}
                className="flex items-center gap-2 px-4 py-2 font-mono text-sm text-zinc-700 dark:text-zinc-300 hover:bg-zinc-100 dark:hover:bg-zinc-800 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                data-testid="prev-page"
              >
                <ChevronLeft size={16} />
                Previous
              </button>
              <div className="flex items-center gap-2">
                {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
                  let pageNum;
                  if (totalPages <= 5) {
                    pageNum = i + 1;
                  } else if (page <= 3) {
                    pageNum = i + 1;
                  } else if (page >= totalPages - 2) {
                    pageNum = totalPages - 4 + i;
                  } else {
                    pageNum = page - 2 + i;
                  }
                  return (
                    <button
                      key={pageNum}
                      onClick={() => setPage(pageNum)}
                      className={`px-3 py-1 font-mono text-sm transition-colors ${
                        page === pageNum
                          ? "bg-zinc-950 dark:bg-white text-white dark:text-zinc-950"
                          : "text-zinc-700 dark:text-zinc-300 hover:bg-zinc-100 dark:hover:bg-zinc-800"
                      }`}
                    >
                      {pageNum}
                    </button>
                  );
                })}
              </div>
              <button
                onClick={() => setPage(Math.min(totalPages, page + 1))}
                disabled={page === totalPages}
                className="flex items-center gap-2 px-4 py-2 font-mono text-sm text-zinc-700 dark:text-zinc-300 hover:bg-zinc-100 dark:hover:bg-zinc-800 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                data-testid="next-page"
              >
                Next
                <ChevronRight size={16} />
              </button>
            </div>
          )}
        </div>
      </main>
    </div>
  );
};

export default ActivityLogPage;
