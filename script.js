document.addEventListener('DOMContentLoaded', async () => {
    const executeButton = document.getElementById('execute-button');
    const sqlQuery = document.getElementById('sql-query');
    const geojsonSource = document.getElementById('geojson-source');
    const tableContainer = document.getElementById('table-container');
    const statusMessage = document.getElementById('status-message');
    const downloadCsvButton = document.getElementById('download-csv-button');

    const query1Button = document.getElementById('query1-button');
    const query2Button = document.getElementById('query2-button');
    const query3Button = document.getElementById('query3-button');
    const query4Button = document.getElementById('query4-button');
    const query5Button = document.getElementById('query5-button');
    const query6Button = document.getElementById('query6-button');
    const query7Button = document.getElementById('query7-button');
    
    // New UI elements
    const validateUrlButton = document.getElementById('validate-url');
    const urlStatusSpan = document.getElementById('url-status');
    const sampleDatasets = document.getElementById('sample-datasets');
    const schemaSection = document.getElementById('schema-section');
    const getSchemaButton = document.getElementById('get-schema');
    const schemaContent = document.getElementById('schema-content');
    const manualMode = document.getElementById('manual-mode');
    const builderMode = document.getElementById('builder-mode');
    const manualQuerySection = document.getElementById('manual-query-section');
    const queryBuilderSection = document.getElementById('query-builder-section');
    const columnSelector = document.getElementById('column-selector');
    const generateQueryButton = document.getElementById('generate-query');
    const addFilterButton = document.getElementById('add-filter');
    const useGroupBy = document.getElementById('use-groupby');
    const groupByColumn = document.getElementById('groupby-column');
    const orderByColumn = document.getElementById('orderby-column');
    
    
    
    const tablePanel = document.getElementById('table-panel');

    let db;
    let currentData = [];
    let sortColumn = null;
    let sortDirection = 'asc';
    let currentPage = 1;
    const rowsPerPage = 10;
    let schemaData = null;
    let modelStartTimes = {}; // Cache for model start times
    let currentStartTime = '1981-01-01T00:00:00Z'; // Default fallback
    let hruData = null; // Cache for HRU data

    async function init() {
        try {
            const JSDELIVR_BUNDLES = window.duckdb.getJsDelivrBundles();
            const bundle = await window.duckdb.selectBundle(JSDELIVR_BUNDLES);
            const worker = await window.duckdb.createWorker(bundle.mainWorker);
            const logger = new window.duckdb.ConsoleLogger();
            db = new window.duckdb.AsyncDuckDB(logger, worker);
            await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
            
            // Load HRU data
            await loadHruData();
            
            executeButton.disabled = false;
            updateStatusMessage();
        } catch (e) {
            statusMessage.textContent = `Error initializing DuckDB: ${e.toString()}`;
            console.error(e);
        }
    }

    // Load HRU data from JSON file
    async function loadHruData() {
        try {
            const response = await fetch('hrus.json');
            hruData = await response.json();
            console.log('Loaded HRU data:', hruData);
        } catch (error) {
            console.warn('Could not load HRU data:', error);
            hruData = [];
        }
    }


    function updateStatusMessage() {
        statusMessage.textContent = 'DuckDB is ready. Enter a query and click Execute.';
        // Update button labels for parquet queries
        query1Button.textContent = 'First 10 Rows';
        query2Button.textContent = 'Count Records';
        query3Button.textContent = 'Column Info';
    }

    await init();


    // Update query when source changes
    function updateQueryWithSource() {
        const currentQuery = sqlQuery.value;
        if (currentQuery && currentQuery.includes('{SOURCE}')) {
            // Query already has placeholder, no need to update
            return;
        }
        
        // If query is empty or doesn't have placeholder, set default query
        if (!currentQuery.trim()) {
            sqlQuery.value = sqlQuery.placeholder;
        }
    }

    // Auto-update query when source field changes
    geojsonSource.addEventListener('input', updateQueryWithSource);
    geojsonSource.addEventListener('change', updateQueryWithSource);

    // Set default query on page load if query is empty
    if (!sqlQuery.value.trim()) {
        sqlQuery.value = sqlQuery.placeholder;
    }



    // Data source management functions
    async function validateUrl() {
        const url = geojsonSource.value.trim();
        if (!url) {
            urlStatusSpan.textContent = '❌';
            return false;
        }
        
        try {
            urlStatusSpan.textContent = '⏳';
            const response = await fetch(url, { method: 'HEAD' });
            if (response.ok) {
                urlStatusSpan.textContent = '✅';
                schemaSection.style.display = 'block';
                return true;
            } else {
                urlStatusSpan.textContent = '❌';
                return false;
            }
        } catch (error) {
            urlStatusSpan.textContent = '❌';
            console.error('URL validation error:', error);
            return false;
        }
    }

    // Schema management functions
    async function getSchema() {
        const sourceUrl = geojsonSource.value.trim();
        if (!sourceUrl) {
            alert('Please enter a Parquet source URL first');
            return;
        }

        try {
            const conn = await db.connect();
            const query = `DESCRIBE SELECT * FROM '${sourceUrl}' LIMIT 1`;
            const result = await conn.query(query);
            
            if (result.numRows > 0) {
                schemaData = result.toArray().map(row => row.toJSON());
                displaySchema(schemaData);
                populateColumnSelectors(schemaData);
                
                // Detect start time for this dataset
                await detectStartTime(conn, sourceUrl);
            }
            
            conn.close();
        } catch (error) {
            console.error('Schema error:', error);
            schemaContent.innerHTML = `<span class="text-danger">Error loading schema: ${error.message}</span>`;
        }
    }

    // Detect the start time for the current dataset
    async function detectStartTime(conn, sourceUrl) {
        try {
            // Check if model column exists to identify dataset
            const hasModelColumn = schemaData.some(col => col.column_name === 'model');
            const hasStartTimeColumn = schemaData.some(col => col.column_name === 'start_time');
            
            // Get basic dataset info
            const infoQuery = `SELECT MIN(ix) as min_ix, MAX(ix) as max_ix, COUNT(*) as total_rows FROM '${sourceUrl}'`;
            const infoResult = await conn.query(infoQuery);
            const dataInfo = infoResult.toArray().map(row => row.toJSON())[0];
            
            console.log('Dataset info:', dataInfo);
            
            if (hasModelColumn) {
                // Get unique models and their minimum ix values
                const modelQuery = `
                    SELECT model, MIN(ix) as min_ix, MAX(ix) as max_ix, COUNT(*) as rows
                    FROM '${sourceUrl}' 
                    GROUP BY model 
                    ORDER BY model
                `;
                const modelResult = await conn.query(modelQuery);
                const models = modelResult.toArray().map(row => row.toJSON());
                
                if (models.length > 0) {
                    // Use the first model's start time as reference
                    const firstModel = models[0];
                    const modelName = firstModel.model;
                    
                    // Try to determine actual start time if start_time column exists
                    if (hasStartTimeColumn) {
                        const startTimeQuery = `
                            SELECT DISTINCT start_time 
                            FROM '${sourceUrl}' 
                            WHERE model = '${modelName}' 
                            LIMIT 1
                        `;
                        const startTimeResult = await conn.query(startTimeQuery);
                        const startTimes = startTimeResult.toArray().map(row => row.toJSON());
                        
                        if (startTimes.length > 0) {
                            currentStartTime = startTimes[0].start_time;
                            modelStartTimes[modelName] = currentStartTime;
                            
                            console.log(`Detected start time for ${modelName}: ${currentStartTime}`);
                            return;
                        }
                    }
                    
                    console.log(`Using default start time. Found models:`, models);
                }
            } else {
                // Single dataset without model column
                if (hasStartTimeColumn) {
                    const startTimeQuery = `SELECT DISTINCT start_time FROM '${sourceUrl}' LIMIT 1`;
                    const startTimeResult = await conn.query(startTimeQuery);
                    const startTimes = startTimeResult.toArray().map(row => row.toJSON());
                    
                    if (startTimes.length > 0) {
                        currentStartTime = startTimes[0].start_time;
                        console.log(`Detected start time: ${currentStartTime}`);
                        return;
                    }
                }
                
                // Try to infer from filename or use common climate model patterns
                const inferredStartTime = inferStartTimeFromUrl(sourceUrl);
                if (inferredStartTime !== currentStartTime) {
                    currentStartTime = inferredStartTime;
                    console.log(`Inferred start time from URL: ${currentStartTime}`);
                } else {
                    console.log(`No start_time column found. Using default start time. Dataset info:`, dataInfo);
                }
            }
        } catch (error) {
            console.warn('Could not detect start time:', error);
        }
    }


    function displaySchema(schema) {
        let html = '<div class="row">';
        schema.forEach((col, index) => {
            const columnName = col.column_name;
            const columnType = col.column_type;
            html += `
                <div class="col-md-4 mb-2">
                    <div class="border rounded p-2 bg-light">
                        <strong>${columnName}</strong><br>
                        <small class="text-muted">${columnType}</small>
                    </div>
                </div>
            `;
        });
        html += '</div>';
        schemaContent.innerHTML = html;
    }

    function populateColumnSelectors(schema) {
        const columns = schema.map(col => col.column_name);
        
        
        // Clear existing options (except "All Columns")
        columnSelector.innerHTML = '<option value="*">* (All Columns)</option>';
        groupByColumn.innerHTML = '<option value="">Select column to group by</option>';
        orderByColumn.innerHTML = '<option value="">Select column...</option>';
        
        // Populate all selectors
        columns.forEach(col => {
            columnSelector.innerHTML += `<option value="${col}">${col}</option>`;
            groupByColumn.innerHTML += `<option value="${col}">${col}</option>`;
            orderByColumn.innerHTML += `<option value="${col}">${col}</option>`;
        });

        // Populate filter column selectors
        document.querySelectorAll('.filter-column').forEach(select => {
            const currentValue = select.value; // Preserve current selection
            select.innerHTML = '<option value="">Select column...</option>';
            columns.forEach(col => {
                select.innerHTML += `<option value="${col}">${col}</option>`;
            });
            
            // Restore selection if it was previously set
            if (currentValue) {
                select.value = currentValue;
            }
            
            // Add change handler for HRU special handling if not already added
            if (!select.dataset.handlerAdded) {
                select.addEventListener('change', () => {
                    const valueContainer = select.closest('.filter-row').querySelector('.filter-value');
                    updateFilterValueInput(select, valueContainer);
                });
                select.dataset.handlerAdded = 'true';
            }
        });
    }

    // DateTime conversion helper functions (now uses dynamic start time)
    function dateToIx(dateStr, hour = 0) {
        const targetDate = new Date(dateStr + 'T' + hour.toString().padStart(2, '0') + ':00:00Z');
        const baseDate = new Date(currentStartTime);
        const diffMs = targetDate.getTime() - baseDate.getTime();
        return Math.floor(diffMs / (1000 * 60 * 60)); // Convert to hours
    }

    function ixToDate(ix) {
        const baseDate = new Date(currentStartTime);
        const targetDate = new Date(baseDate.getTime() + (ix * 60 * 60 * 1000));
        return targetDate.toISOString();
    }

    // Get the current start time as SQL timestamp string
    function getCurrentStartTimeSQL() {
        return `'${currentStartTime.replace('Z', '')}'::TIMESTAMP`;
    }

    // Infer start time from URL patterns (common climate models)
    function inferStartTimeFromUrl(url) {
        // Common climate model patterns
        const patterns = [
            // CCSM4 models typically start in 1950 or 2006 for RCP scenarios
            { pattern: /ccsm4.*rcp/i, startTime: '2006-01-01T00:00:00Z' },
            { pattern: /ccsm4/i, startTime: '1950-01-01T00:00:00Z' },
            
            // CESM models
            { pattern: /cesm.*rcp/i, startTime: '2006-01-01T00:00:00Z' },
            { pattern: /cesm/i, startTime: '1850-01-01T00:00:00Z' },
            
            // GFDL models
            { pattern: /gfdl.*rcp/i, startTime: '2006-01-01T00:00:00Z' },
            { pattern: /gfdl/i, startTime: '1861-01-01T00:00:00Z' },
            
            // Common historical period
            { pattern: /historical/i, startTime: '1850-01-01T00:00:00Z' },
            
            // RCP scenarios typically start in 2006
            { pattern: /rcp\d+/i, startTime: '2006-01-01T00:00:00Z' },
        ];
        
        for (const { pattern, startTime } of patterns) {
            if (pattern.test(url)) {
                console.log(`Matched pattern ${pattern} in URL, using start time: ${startTime}`);
                return startTime;
            }
        }
        
        // Default fallback
        return currentStartTime;
    }



    // Query builder functions
    function toggleQueryMode() {
        if (builderMode.checked) {
            manualQuerySection.style.display = 'none';
            queryBuilderSection.style.display = 'block';
        } else {
            manualQuerySection.style.display = 'block';
            queryBuilderSection.style.display = 'none';
        }
    }

    function addFilterRow() {
        const filterBuilder = document.getElementById('filter-builder');
        const newFilterRow = document.createElement('div');
        newFilterRow.className = 'filter-row mb-2';
        newFilterRow.innerHTML = `
            <div class="row g-2">
                <div class="col-md-3">
                    <select class="form-select filter-column">
                        <option value="">Select column...</option>
                        ${schemaData ? schemaData.map(col => `<option value="${col.column_name}">${col.column_name}</option>`).join('') : ''}
                    </select>
                </div>
                <div class="col-md-2">
                    <select class="form-select filter-operator">
                        <option value="=">=</option>
                        <option value="!=">!=</option>
                        <option value=">">></option>
                        <option value="<"><</option>
                        <option value=">=">>=</option>
                        <option value="<="><=</option>
                        <option value="LIKE">LIKE</option>
                    </select>
                </div>
                <div class="col-md-3">
                    <input type="text" class="form-control filter-value" placeholder="Value">
                </div>
                <div class="col-md-2">
                    <select class="form-select filter-logic">
                        <option value="AND">AND</option>
                        <option value="OR">OR</option>
                    </select>
                </div>
                <div class="col-md-2">
                    <button type="button" class="btn btn-outline-danger btn-sm remove-filter">Remove</button>
                </div>
            </div>
        `;
        filterBuilder.appendChild(newFilterRow);
        
        // Add column change handler for HRU special handling
        const columnSelect = newFilterRow.querySelector('.filter-column');
        const valueInput = newFilterRow.querySelector('.filter-value');
        
        columnSelect.addEventListener('change', () => {
            updateFilterValueInput(columnSelect, valueInput);
        });
        
        // Add remove functionality
        newFilterRow.querySelector('.remove-filter').addEventListener('click', () => {
            newFilterRow.remove();
        });
    }

    // Update filter value input based on selected column
    function updateFilterValueInput(columnSelect, valueContainer) {
        const selectedColumn = columnSelect.value;
        
        // Check if this is an HRU-related column
        if (selectedColumn === 'hru' || selectedColumn.toLowerCase().includes('hru')) {
            // Replace input with select dropdown for HRU values
            if (hruData && hruData.length > 0) {
                const hruOptions = hruData.map(hru => 
                    `<option value="${hru.hru_name}">${hru.Label} (${hru.hru_name})</option>`
                ).join('');
                
                valueContainer.outerHTML = `
                    <select class="form-control filter-value">
                        <option value="">Select HRU...</option>
                        ${hruOptions}
                    </select>
                `;
            }
        } else {
            // Restore regular text input
            if (valueContainer.tagName === 'SELECT') {
                valueContainer.outerHTML = `<input type="text" class="form-control filter-value" placeholder="Value">`;
            }
        }
    }

    function generateQuery() {
        const sourceUrl = geojsonSource.value.trim();
        if (!sourceUrl) {
            alert('Please enter a Parquet source URL');
            return;
        }

        // Get selected columns
        const selectedColumns = Array.from(columnSelector.selectedOptions).map(option => option.value);
        let selectClause = selectedColumns.length > 0 ? selectedColumns.join(', ') : '*';
        
        // Always add datetime conversion column
        const datetimeExpression = `${getCurrentStartTimeSQL()} + INTERVAL (ix) HOUR as datetime_col`;
        if (selectClause === '*') {
            selectClause = `*, ${datetimeExpression}`;
        } else {
            selectClause += `, ${datetimeExpression}`;
        }

        // Build WHERE clause
        const filterRows = document.querySelectorAll('.filter-row');
        let whereClause = '';
        const conditions = [];
        
        filterRows.forEach((row, index) => {
            const column = row.querySelector('.filter-column').value;
            const operator = row.querySelector('.filter-operator').value;
            const value = row.querySelector('.filter-value').value.trim();
            const logic = row.querySelector('.filter-logic').value;
            
            if (column && value) {
                let condition = `${column} ${operator} `;
                if (operator === 'LIKE') {
                    condition += `'%${value}%'`;
                } else if (isNaN(value)) {
                    condition += `'${value}'`;
                } else {
                    condition += value;
                }
                
                if (conditions.length > 0) {
                    condition = ` ${logic} ${condition}`;
                }
                conditions.push(condition);
            }
        });
        
        
        if (conditions.length > 0) {
            whereClause = ' WHERE ' + conditions.join('');
        }

        // Build GROUP BY clause
        let groupByClause = '';
        if (useGroupBy.checked && groupByColumn.value) {
            groupByClause = ` GROUP BY ${groupByColumn.value}`;
        }

        // Build ORDER BY clause
        let orderByClause = '';
        if (orderByColumn.value) {
            const direction = document.getElementById('orderby-direction').value;
            orderByClause = ` ORDER BY ${orderByColumn.value} ${direction}`;
        }

        // Build LIMIT clause
        let limitClause = '';
        const limitValue = document.getElementById('limit-value').value;
        if (limitValue) {
            limitClause = ` LIMIT ${limitValue}`;
        }

        // Construct final query
        const query = `SELECT ${selectClause} FROM '${sourceUrl}'${whereClause}${groupByClause}${orderByClause}${limitClause}`;
        
        // Set the query in the manual textarea and switch to manual mode
        sqlQuery.value = query;
        manualMode.checked = true;
        toggleQueryMode();
    }

    // Function to substitute source URL in query templates
    function substituteSource(queryTemplate) {
        const sourceUrl = geojsonSource.value.trim();
        if (!sourceUrl) {
            alert('Please enter a Parquet source URL');
            return null;
        }
        return queryTemplate.replace(/\{SOURCE\}/g, sourceUrl);
    }

    // Event listeners for new functionality
    validateUrlButton.addEventListener('click', validateUrl);
    
    sampleDatasets.addEventListener('change', (e) => {
        if (e.target.value) {
            geojsonSource.value = e.target.value;
            urlStatusSpan.textContent = '🔍';
            schemaSection.style.display = 'block';
        }
    });
    
    getSchemaButton.addEventListener('click', getSchema);
    
    manualMode.addEventListener('change', toggleQueryMode);
    builderMode.addEventListener('change', toggleQueryMode);
    
    generateQueryButton.addEventListener('click', generateQuery);
    
    addFilterButton.addEventListener('click', addFilterRow);
    
    useGroupBy.addEventListener('change', () => {
        groupByColumn.disabled = !useGroupBy.checked;
    });
    

    // Add remove functionality to initial filter row
    document.querySelector('.remove-filter').addEventListener('click', (e) => {
        e.target.closest('.filter-row').remove();
    });

    // Add HRU handling to the initial filter row
    const initialColumnSelect = document.querySelector('.filter-column');
    if (initialColumnSelect) {
        initialColumnSelect.addEventListener('change', () => {
            const valueContainer = initialColumnSelect.closest('.filter-row').querySelector('.filter-value');
            updateFilterValueInput(initialColumnSelect, valueContainer);
        });
    }

    // Event listeners for pre-defined query buttons
    query1Button.addEventListener('click', () => {
        sqlQuery.value = query1Button.dataset.query;
        executeButton.click();
    });

    query2Button.addEventListener('click', () => {
        sqlQuery.value = query2Button.dataset.query;
        executeButton.click();
    });

    query3Button.addEventListener('click', () => {
        sqlQuery.value = query3Button.dataset.query;
        executeButton.click();
    });

    query4Button.addEventListener('click', () => {
        const sourceUrl = geojsonSource.value.trim();
        if (!sourceUrl) {
            alert('Please enter a Parquet source URL first');
            return;
        }
        const dynamicQuery = `SELECT ix, ${getCurrentStartTimeSQL()} + INTERVAL (ix) HOUR as datetime_col FROM '${sourceUrl}' LIMIT 10;`;
        sqlQuery.value = dynamicQuery;
        executeButton.click();
    });

    query5Button.addEventListener('click', () => {
        sqlQuery.value = query5Button.dataset.query;
        executeButton.click();
    });

    query6Button.addEventListener('click', () => {
        sqlQuery.value = query6Button.dataset.query;
        executeButton.click();
    });

    query7Button.addEventListener('click', () => {
        sqlQuery.value = query7Button.dataset.query;
        executeButton.click();
    });

    executeButton.addEventListener('click', async () => {
        let query = sqlQuery.value;
        if (!query) {
            alert('Please enter a SQL query.');
            return;
        }

        // Substitute {SOURCE} placeholders with actual URL
        if (query.includes('{SOURCE}')) {
            const sourceUrl = geojsonSource.value.trim();
            if (!sourceUrl) {
                alert('Please enter a Parquet source URL');
                return;
            }
            query = query.replace(/\{SOURCE\}/g, sourceUrl);
        }

        try {
            const conn = await db.connect();
            const result = await conn.query(query);
            conn.close();

            if (result.numRows > 0) {
                currentData = result.toArray().map(row => row.toJSON());
                console.log('Query executed, new data:', currentData);
                sortColumn = null;
                sortDirection = 'asc';
                currentPage = 1;
                renderTable();
                downloadCsvButton.style.display = 'block'; // Show the download button
            } else {
                tableContainer.innerHTML = '<p>No results found.</p>';
                downloadCsvButton.style.display = 'none'; // Hide the download button
            }
        } catch (error) {
            console.error(error);
            tableContainer.innerHTML = `<p>Error: ${error.message}</p>`;
        }
    });

    function renderTable() {
        tableContainer.innerHTML = ''; // Clear previous table

        if (currentData.length === 0) {
            tableContainer.innerHTML = '<p>No results found.</p>';
            return;
        }

        const columns = Object.keys(currentData[0]);

        // Sort data
        let sortedData = [...currentData];
        if (sortColumn) {
            sortedData.sort((a, b) => {
                const valA = a[sortColumn];
                const valB = b[sortColumn];

                if (valA < valB) return sortDirection === 'asc' ? -1 : 1;
                if (valA > valB) return sortDirection === 'asc' ? 1 : -1;
                return 0;
            });
        }

        // Paginate data
        const totalPages = Math.ceil(sortedData.length / rowsPerPage);
        const startIndex = (currentPage - 1) * rowsPerPage;
        const endIndex = startIndex + rowsPerPage;
        const paginatedData = sortedData.slice(startIndex, endIndex);

        const table = d3.select(tableContainer).append('table').attr('class', 'table table-striped table-hover');
        const thead = table.append('thead');
        const tbody = table.append('tbody');

        // Append the header row
        thead.append('tr')
            .selectAll('th')
            .data(columns)
            .enter()
            .append('th')
            .text(d => d)
            .on('click', (event, column) => {
                if (sortColumn === column) {
                    sortDirection = sortDirection === 'asc' ? 'desc' : 'asc';
                } else {
                    sortColumn = column;
                    sortDirection = 'asc';
                }
                renderTable();
            });

        // Append the data rows
        const rows = tbody.selectAll('tr')
            .data(paginatedData)
            .enter()
            .append('tr');

        rows.selectAll('td')
            .data(row => columns.map(column => row[column]))
            .enter()
            .append('td')
            .html(d => {
                if (typeof d === 'number') {
                    return d3.format(".4~f")(d);
                }
                return d;
            });

        // Pagination controls
        const paginationDiv = d3.select(tableContainer).append('div').attr('class', 'pagination mt-3');

        paginationDiv.append('button')
            .attr('class', 'btn btn-secondary me-2')
            .attr('disabled', currentPage === 1 ? true : null)
            .text('Previous')
            .on('click', () => {
                currentPage--;
                renderTable();
            });

        paginationDiv.append('span').text(`Page ${currentPage} of ${totalPages}`);

        paginationDiv.append('button')
            .attr('class', 'btn btn-secondary ms-2')
            .attr('disabled', currentPage === totalPages ? true : null)
            .text('Next')
            .on('click', () => {
                currentPage++;
                renderTable();
            });
    }

    downloadCsvButton.addEventListener('click', () => {
        if (currentData.length === 0) {
            alert('No data to download.');
            return;
        }

        const columns = Object.keys(currentData[0]);
        let csvContent = columns.join(',') + '\n';

        currentData.forEach(row => {
            const rowValues = columns.map(col => {
                let value = row[col];
                if (typeof value === 'string' && value.includes(',')) {
                    value = `"${value.replace(/"/g, '""')}"`; // Enclose in double quotes and escape existing double quotes
                }
                return value;
            });
            csvContent += rowValues.join(',') + '\n';
        });

        const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
        const link = document.createElement('a');
        if (link.download !== undefined) { // Feature detection for download attribute
            const url = URL.createObjectURL(blob);
            link.setAttribute('href', url);
            link.setAttribute('download', 'query_results.csv');
            link.style.visibility = 'hidden';
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            URL.revokeObjectURL(url);
        } else {
            alert('Your browser does not support downloading files directly. Please copy the data manually.');
        }
    });
});