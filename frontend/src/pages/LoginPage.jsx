import { useState } from "react";
import { motion } from "framer-motion";
import { Mail, Lock, ArrowRight, AlertCircle } from "lucide-react";
import { useAuth } from "../context/AuthContext";

const LoginPage = () => {
  const { login, error } = useAuth();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!email.trim() || !password.trim()) return;
    setIsSubmitting(true);
    await login(email, password);
    setIsSubmitting(false);
  };

  return (
    <div className="min-h-screen flex bg-white dark:bg-zinc-950">
      {/* Left side - Hero Image */}
      <div className="hidden lg:flex lg:w-1/2 relative">
        <img
          src="https://static.prod-images.emergentagent.com/jobs/9b0b82cb-43a7-4b6e-af0a-4fca2310be87/images/646843d7fbd670545713fa281223a4796b73e168ba8187b0b49bc905e3e7c4c5.png"
          alt="Swiss Design Background"
          className="absolute inset-0 w-full h-full object-cover"
        />
        <div className="absolute inset-0 bg-black/30 flex items-end p-12">
          <div className="text-white">
            <h1 className="font-heading text-4xl font-black tracking-tighter mb-2">
              SCRAPER.
            </h1>
            <p className="font-mono text-sm opacity-80 tracking-wide">
              DISTRIBUTED WEB SCRAPING SERVICE
            </p>
          </div>
        </div>
      </div>

      {/* Right side - Login Form */}
      <div className="flex-1 flex items-center justify-center p-8">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          className="w-full max-w-md"
        >
          {/* Mobile Logo */}
          <div className="lg:hidden mb-12 text-center">
            <h1 className="font-heading text-3xl font-black tracking-tighter text-zinc-950 dark:text-white">
              SCRAPER.
            </h1>
            <p className="font-mono text-xs text-zinc-500 dark:text-zinc-400 tracking-wide mt-1">
              DISTRIBUTED WEB SCRAPING SERVICE
            </p>
          </div>

          <div className="space-y-8">
            <div>
              <p className="font-mono text-xs uppercase tracking-[0.2em] text-zinc-500 dark:text-zinc-400 mb-2">
                AUTHENTICATION
              </p>
              <h2 className="font-heading text-2xl font-bold tracking-tight text-zinc-950 dark:text-white">
                Sign in to your account
              </h2>
            </div>

            {error && (
              <motion.div
                initial={{ opacity: 0, x: -10 }}
                animate={{ opacity: 1, x: 0 }}
                className="flex items-center gap-3 p-4 bg-red-100 dark:bg-red-500/10 border border-red-200 dark:border-red-500/20 text-red-800 dark:text-red-400"
                data-testid="login-error"
              >
                <AlertCircle size={18} strokeWidth={1.5} />
                <span className="font-mono text-sm">{error}</span>
              </motion.div>
            )}

            <form onSubmit={handleSubmit} className="space-y-6">
              <div>
                <label className="font-mono text-xs uppercase tracking-[0.15em] text-zinc-600 dark:text-zinc-400 mb-2 block">
                  EMAIL
                </label>
                <div className="relative">
                  <Mail
                    className="absolute left-4 top-1/2 -translate-y-1/2 text-zinc-400"
                    size={18}
                    strokeWidth={1.5}
                  />
                  <input
                    type="email"
                    value={email}
                    onChange={(e) => setEmail(e.target.value)}
                    placeholder="admin@example.com"
                    className="w-full pl-12 pr-4 py-4 font-mono text-sm bg-white dark:bg-zinc-900 border border-zinc-300 dark:border-zinc-700 rounded-none text-zinc-950 dark:text-white placeholder-zinc-400 focus:outline-none focus:ring-2 focus:ring-black dark:focus:ring-white"
                    data-testid="email-input"
                  />
                </div>
              </div>

              <div>
                <label className="font-mono text-xs uppercase tracking-[0.15em] text-zinc-600 dark:text-zinc-400 mb-2 block">
                  PASSWORD
                </label>
                <div className="relative">
                  <Lock
                    className="absolute left-4 top-1/2 -translate-y-1/2 text-zinc-400"
                    size={18}
                    strokeWidth={1.5}
                  />
                  <input
                    type="password"
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    placeholder="••••••••"
                    className="w-full pl-12 pr-4 py-4 font-mono text-sm bg-white dark:bg-zinc-900 border border-zinc-300 dark:border-zinc-700 rounded-none text-zinc-950 dark:text-white placeholder-zinc-400 focus:outline-none focus:ring-2 focus:ring-black dark:focus:ring-white"
                    data-testid="password-input"
                  />
                </div>
              </div>

              <button
                type="submit"
                disabled={isSubmitting || !email.trim() || !password.trim()}
                className="w-full flex items-center justify-center gap-3 py-4 bg-zinc-950 dark:bg-white text-white dark:text-zinc-950 font-mono font-bold text-sm tracking-wide hover:bg-zinc-800 dark:hover:bg-zinc-200 disabled:opacity-50 disabled:cursor-not-allowed transition-colors duration-150"
                data-testid="login-submit-btn"
              >
                {isSubmitting ? (
                  <div className="flex items-center gap-2">
                    <div
                      className="w-2 h-2 bg-current rounded-full animate-bounce"
                      style={{ animationDelay: "0ms" }}
                    />
                    <div
                      className="w-2 h-2 bg-current rounded-full animate-bounce"
                      style={{ animationDelay: "150ms" }}
                    />
                    <div
                      className="w-2 h-2 bg-current rounded-full animate-bounce"
                      style={{ animationDelay: "300ms" }}
                    />
                    <span className="ml-2">SIGNING IN...</span>
                  </div>
                ) : (
                  <>
                    SIGN IN
                    <ArrowRight size={18} strokeWidth={1.5} />
                  </>
                )}
              </button>
            </form>

            <p className="font-mono text-xs text-center text-zinc-400">
              Contact your administrator for account access.
            </p>
          </div>
        </motion.div>
      </div>
    </div>
  );
};

export default LoginPage;
