echo Getting Eventbrite data
echo.

python main.py --output "\\na-stor02\VisualStudioCommunity$\Single Customer View\Data\Sources\Eventbrite_full_data.xlsx" --answers-output "\\na-stor02\VisualStudioCommunity$\Single Customer View\Data\Sources\Eventbrite_answers.xlsx"

move "\\na-stor02\VisualStudioCommunity$\Single Customer View\Integration Services\SCV Solutions\Eventbrite\*.csv" "\\na-stor02\VisualStudioCommunity$\Single Customer View\Data\Other data\Events"

pause