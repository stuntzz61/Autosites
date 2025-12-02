import React from 'react'
import ReactDOM from 'react-dom/client'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { BrowserRouter } from 'react-router-dom'
import { Toaster } from 'react-hot-toast'
import App from './App'
import { TelegramProvider } from './contexts/TelegramContext'
import './index.css'

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
    },
  },
})

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <BrowserRouter basename="/app">
        <TelegramProvider>
          <App />
          <Toaster
            position="top-center"
            toastOptions={{
              duration: 3000,
              style: {
                background: 'var(--tg-theme-bg-color)',
                color: 'var(--tg-theme-text-color)',
                borderRadius: '12px',
                padding: '12px 16px',
              },
            }}
          />
        </TelegramProvider>
      </BrowserRouter>
    </QueryClientProvider>
  </React.StrictMode>,
)
