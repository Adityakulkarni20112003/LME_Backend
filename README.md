# LME Aluminum Price Scraper

This application scrapes aluminum price data from investing.com and provides API endpoints to access the data.

## Local Development

1. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Run the application:
   ```
   python app.py
   ```

3. Access the API at `http://localhost:5003`

## API Endpoints

- `/data` - Get the latest scraped data
- `/stream` - Server-sent events stream for real-time updates
- `/download` - Download the CSV data
- `/status` - Check the application status

## Deployment

### Heroku Deployment

1. Create a Heroku account and install the Heroku CLI
2. Login to Heroku:
   ```
   heroku login
   ```

3. Create a new Heroku app:
   ```
   heroku create your-app-name
   ```

4. Add the Chrome buildpack:
   ```
   heroku buildpacks:add https://github.com/heroku/heroku-buildpack-google-chrome
   ```

5. Add the Python buildpack:
   ```
   heroku buildpacks:add heroku/python
   ```

6. Deploy the application:
   ```
   git push heroku main
   ```

### Railway Deployment

1. Create a Railway account
2. Connect your GitHub repository
3. Add the environment variables:
   - `PORT=8080`
4. Deploy the application

## Important Notes

This application uses Selenium with Chrome WebDriver to scrape data. Make sure the hosting platform supports this setup.

For Vercel deployment, this application is NOT suitable as it requires a long-running process and background threads. 