import {
  createContext,
  useContext,
  useState,
  useEffect,
  useCallback,
} from "react";
import { authAPI } from "../utils/api";

const AuthContext = createContext(null);

export const useAuth = () => {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error("useAuth must be used within an AuthProvider");
  }
  return context;
};

export const AuthProvider = ({ children }) => {
  const [user, setUser] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const fetchUser = useCallback(async () => {
    const token = localStorage.getItem("jwt_token");
    if (!token) {
      setLoading(false);
      return;
    }

    try {
      const response = await authAPI.getCurrentUser();
      setUser(response.data);
      setError(null);
    } catch (err) {
      console.error("Failed to fetch user:", err);
      localStorage.removeItem("jwt_token");
      setUser(null);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchUser();
  }, [fetchUser]);

  const login = async (email, password) => {
    try {
      setError(null);
      const response = await authAPI.login(email, password);
      const { access_token } = response.data;
      localStorage.setItem("jwt_token", access_token);
      await fetchUser();
      return true;
    } catch (err) {
      const message = err.response?.data?.detail || "Authentication failed";
      setError(message);
      return false;
    }
  };

  const logout = async () => {
    try {
      // Call logout endpoint to log the activity
      await authAPI.logout();
    } catch (err) {
      // Continue with logout even if the API call fails
      console.error("Logout API call failed:", err);
    }
    localStorage.removeItem("jwt_token");
    setUser(null);
  };

  const value = {
    user,
    loading,
    error,
    login,
    logout,
    isAuthenticated: !!user,
  };

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
};
