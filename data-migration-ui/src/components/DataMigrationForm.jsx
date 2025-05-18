import React, { useState } from 'react';
import {
  Box,
  TextField,
  Button,
  Typography,
  Paper,
  Switch,
  FormControlLabel,
  Container,
  Grid,
  Alert,
  CircularProgress,
  Snackbar,
} from '@mui/material';
import { styled } from '@mui/material/styles';
import { triggerDag } from '../services/airflowService';

const StyledPaper = styled(Paper)(({ theme }) => ({
  padding: theme.spacing(4),
  margin: theme.spacing(2),
  borderRadius: '12px',
  boxShadow: '0 4px 6px rgba(0, 0, 0, 0.1)',
}));

export default function DataMigrationForm() {
  const [formData, setFormData] = useState({
    source: {
      host: '',
      port: '',
      database: '',
      username: '',
      password: '',
    },
    target: {
      host: '',
      port: '',
      database: '',
      username: '',
      password: '',
    },
    includeDataIngestion: false,
  });

  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(false);

  const handleChange = (section, field) => (event) => {
    setFormData({
      ...formData,
      [section]: {
        ...formData[section],
        [field]: event.target.value,
      },
    });
  };

  const handleSwitchChange = (event) => {
    setFormData({
      ...formData,
      includeDataIngestion: event.target.checked,
    });
  };

  const handleSubmit = async (event) => {
    event.preventDefault();
    setIsLoading(true);
    setError(null);
    
    try {
      const config = {
        source: {
          ...formData.source,
          type: 'mysql'
        },
        target: {
          ...formData.target,
          type: 'sqlserver'
        },
        includeDataIngestion: formData.includeDataIngestion
      };

      await triggerDag('spark_submit_via_docker', config);
      setSuccess(true);
    } catch (err) {
      setError(err.message);
    } finally {
      setIsLoading(false);
    }
  };

  const handleCloseSnackbar = () => {
    setError(null);
    setSuccess(false);
  };

  return (
    <Container maxWidth="lg">
      <Box sx={{ my: 4 }}>
        <Typography variant="h4" component="h1" gutterBottom align="center">
          Data Migration Tool
        </Typography>
        <form onSubmit={handleSubmit}>
          <Grid container spacing={3}>
            {/* Source Configuration */}
            <Grid item xs={12} md={6}>
              <StyledPaper>
                <Typography variant="h6" gutterBottom>
                  Source Configuration (MySQL)
                </Typography>
                <Box sx={{ mt: 2 }}>
                  <TextField
                    fullWidth
                    label="Host"
                    value={formData.source.host}
                    onChange={handleChange('source', 'host')}
                    margin="normal"
                  />
                  <TextField
                    fullWidth
                    label="Port"
                    value={formData.source.port}
                    onChange={handleChange('source', 'port')}
                    margin="normal"
                  />
                  <TextField
                    fullWidth
                    label="Database"
                    value={formData.source.database}
                    onChange={handleChange('source', 'database')}
                    margin="normal"
                  />
                  <TextField
                    fullWidth
                    label="Username"
                    value={formData.source.username}
                    onChange={handleChange('source', 'username')}
                    margin="normal"
                  />
                  <TextField
                    fullWidth
                    label="Password"
                    type="password"
                    value={formData.source.password}
                    onChange={handleChange('source', 'password')}
                    margin="normal"
                  />
                </Box>
              </StyledPaper>
            </Grid>

            {/* Target Configuration */}
            <Grid item xs={12} md={6}>
              <StyledPaper>
                <Typography variant="h6" gutterBottom>
                  Target Configuration (SQL Server)
                </Typography>
                <Box sx={{ mt: 2 }}>
                  <TextField
                    fullWidth
                    label="Host"
                    value={formData.target.host}
                    onChange={handleChange('target', 'host')}
                    margin="normal"
                  />
                  <TextField
                    fullWidth
                    label="Port"
                    value={formData.target.port}
                    onChange={handleChange('target', 'port')}
                    margin="normal"
                  />
                  <TextField
                    fullWidth
                    label="Database"
                    value={formData.target.database}
                    onChange={handleChange('target', 'database')}
                    margin="normal"
                  />
                  <TextField
                    fullWidth
                    label="Username"
                    value={formData.target.username}
                    onChange={handleChange('target', 'username')}
                    margin="normal"
                  />
                  <TextField
                    fullWidth
                    label="Password"
                    type="password"
                    value={formData.target.password}
                    onChange={handleChange('target', 'password')}
                    margin="normal"
                  />
                </Box>
              </StyledPaper>
            </Grid>
          </Grid>

          <StyledPaper>
            <FormControlLabel
              control={
                <Switch
                  checked={formData.includeDataIngestion}
                  onChange={handleSwitchChange}
                  color="primary"
                />
              }
              label="Include Data Ingestion"
            />
            <Box sx={{ mt: 2 }}>
              <Alert severity="info">
                {formData.includeDataIngestion
                  ? 'Both metadata and data will be migrated'
                  : 'Only metadata will be migrated'}
              </Alert>
            </Box>
          </StyledPaper>

          <Box sx={{ mt: 3, textAlign: 'center' }}>
            <Button
              type="submit"
              variant="contained"
              color="primary"
              size="large"
              sx={{ minWidth: 200 }}
              disabled={isLoading}
            >
              {isLoading ? (
                <CircularProgress size={24} color="inherit" />
              ) : (
                'Start Migration'
              )}
            </Button>
          </Box>
        </form>

        <Snackbar
          open={!!error}
          autoHideDuration={6000}
          onClose={handleCloseSnackbar}
          message={error}
          severity="error"
        />

        <Snackbar
          open={success}
          autoHideDuration={6000}
          onClose={handleCloseSnackbar}
          message="DAG triggered successfully!"
          severity="success"
        />
      </Box>
    </Container>
  );
}