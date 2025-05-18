import React from 'react';
import { Box, Typography, Grid, Paper } from '@mui/material';
import { styled } from '@mui/material/styles';

const StyledPaper = styled(Paper)(({ theme }) => ({
  padding: theme.spacing(3),
  textAlign: 'center',
  color: theme.palette.text.secondary,
}));

export default function Dashboard() {
  return (
    <Box sx={{ flexGrow: 1, p: 3 }}>
      <Typography variant="h4" gutterBottom>
        Dashboard
      </Typography>
      <Grid container spacing={3}>
        <Grid item xs={12} md={6} lg={3}>
          <StyledPaper>
            <Typography variant="h6">Source Systems</Typography>
            {/* Add content */}
          </StyledPaper>
        </Grid>
        <Grid item xs={12} md={6} lg={3}>
          <StyledPaper>
            <Typography variant="h6">Target Systems</Typography>
            {/* Add content */}
          </StyledPaper>
        </Grid>
        <Grid item xs={12} md={6} lg={3}>
          <StyledPaper>
            <Typography variant="h6">Active Pipelines</Typography>
            {/* Add content */}
          </StyledPaper>
        </Grid>
        <Grid item xs={12} md={6} lg={3}>
          <StyledPaper>
            <Typography variant="h6">Transformations</Typography>
            {/* Add content */}
          </StyledPaper>
        </Grid>
      </Grid>
    </Box>
  );
}