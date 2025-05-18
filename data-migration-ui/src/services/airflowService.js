const AIRFLOW_API_BASE_URL = import.meta.env.VITE_AIRFLOW_API_URL;
const AIRFLOW_USERNAME = import.meta.env.VITE_AIRFLOW_USERNAME || 'airflow';
const AIRFLOW_PASSWORD = import.meta.env.VITE_AIRFLOW_PASSWORD || 'airflow';

export const triggerDag = async (dagId, config) => {
  try {
    // Create base64 encoded credentials
    const base64Credentials = btoa(`${AIRFLOW_USERNAME}:${AIRFLOW_PASSWORD}`);

    const response = await fetch(`${AIRFLOW_API_BASE_URL}/dags/${dagId}/dagRuns`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Basic ${base64Credentials}`
      },
      mode: 'cors',
      credentials: 'include',
      body: JSON.stringify({
        conf: config,
      }),
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      throw new Error(errorData.detail || `HTTP error! status: ${response.status}`);
    }

    return await response.json();
  } catch (error) {
    console.error('Error triggering DAG:', error);
    throw error;
  }
};